//
//  KFHLSUploader.m
//  FFmpegEncoder
//
//  Created by Christopher Ballinger on 12/20/13.
//  Copyright (c) 2013 Christopher Ballinger. All rights reserved.
//

#import "KFHLSUploader.h"
#import "KFS3Stream.h"
#import "KFUser.h"
#import "KFLog.h"
#import "KFAPIClient.h"
#import "KFAWSCredentialsProvider.h"
#import <AWSS3/AWSS3.h>

static NSString * const kFileNameKey = @"fileName";
static NSString * const kFileStartDateKey = @"startDate";

static NSString * const kUploadStateQueued = @"queued";
static NSString * const kUploadStateFinished = @"finished";
static NSString * const kUploadStateUploading = @"uploading";
static NSString * const kUploadStateFailed = @"failed";

static NSString * const kKFS3TransferManagerKey = @"kKFS3TransferManagerKey";
static NSString * const kKFS3Key = @"kKFS3Key";


@interface KFHLSUploader()
@property (nonatomic) NSUInteger numbersOffset;
@property (nonatomic, strong) NSMutableDictionary *queuedSegments;
@property (nonatomic) NSUInteger nextSegmentIndexToUpload;
@property (nonatomic, strong) AWSS3TransferManager *transferManager;
@property (nonatomic, strong) AWSS3 *s3;
@property (nonatomic, strong) KFDirectoryWatcher *directoryWatcher;
@property (atomic, strong) NSMutableDictionary *files;
@property (nonatomic) BOOL isFinishedRecording;
@end

@implementation KFHLSUploader

- (id) initWithDirectoryPath:(NSString *)directoryPath stream:(KFS3Stream *)stream {
    if (self = [super init]) {
        self.stream = stream;
        _directoryPath = [directoryPath copy];
        dispatch_async(dispatch_get_main_queue(), ^{
            self.directoryWatcher = [KFDirectoryWatcher watchFolderWithPath:_directoryPath delegate:self];
        });
        _files = [NSMutableDictionary dictionary];
        _scanningQueue = dispatch_queue_create("KFHLSUploader Scanning Queue", DISPATCH_QUEUE_SERIAL);
        _callbackQueue = dispatch_queue_create("KFHLSUploader Callback Queue", DISPATCH_QUEUE_SERIAL);
        _queuedSegments = [NSMutableDictionary dictionaryWithCapacity:5];
        _numbersOffset = 0;
        _nextSegmentIndexToUpload = 0;
        _isFinishedRecording = NO;
        
        AWSRegionType region = [KFAWSCredentialsProvider regionTypeForRegion:stream.awsRegion];
        KFAWSCredentialsProvider *awsCredentialsProvider = [[KFAWSCredentialsProvider alloc] initWithStream:stream];
        AWSServiceConfiguration *configuration = [[AWSServiceConfiguration alloc] initWithRegion:region
                                                                             credentialsProvider:awsCredentialsProvider];
        
        [AWSS3TransferManager registerS3TransferManagerWithConfiguration:configuration forKey:kKFS3TransferManagerKey];
        [AWSS3 registerS3WithConfiguration:configuration forKey:kKFS3Key];
        
        self.transferManager = [AWSS3TransferManager S3TransferManagerForKey:kKFS3TransferManagerKey];
        self.s3 = [AWSS3 S3ForKey:kKFS3Key];
    }
    return self;
}

- (void) finishedRecording {
    self.isFinishedRecording = YES;
    
    // upload final segment -
    [self uploadLastSegment];
}

- (void) setUseSSL:(BOOL)useSSL {
    _useSSL = useSSL;
}

- (void) uploadNextSegment {
    NSArray *contents = [[NSFileManager defaultManager] contentsOfDirectoryAtPath:self.directoryPath error:nil];
    NSUInteger tsFileCount = 0;
    for (NSString *fileName in contents) {
        if ([[fileName pathExtension] isEqualToString:@"ts"]) {
            tsFileCount++;
        }
    }
    
    NSDictionary *segmentInfo = [_queuedSegments objectForKey:@(_nextSegmentIndexToUpload)];
    
    // Skip uploading files that are currently being written
    if (tsFileCount == 1 && !self.isFinishedRecording) {
        DDLogInfo(@"Skipping upload of ts file currently being recorded: %@ %@", segmentInfo, contents);
        return;
    }
    
    NSString *fileName = [segmentInfo objectForKey:kFileNameKey];
    NSString *fileUploadState = [_files objectForKey:fileName];
    if (![fileUploadState isEqualToString:kUploadStateQueued]) {
        DDLogVerbose(@"Trying to upload file that isn't queued (%@): %@", fileUploadState, segmentInfo);
        return;
    }
    
    if ([fileUploadState isEqualToString:kUploadStateUploading]) {
        NSLog(@"already uploading");
        return;
    }
    else {
        [_files setObject:kUploadStateUploading forKey:fileName];
        NSString *filePath = [_directoryPath stringByAppendingPathComponent:fileName];
        NSString *key = [self awsKeyForStream:self.stream fileName:fileName];
        
        AWSS3TransferManagerUploadRequest *uploadRequest = [AWSS3TransferManagerUploadRequest new];
        uploadRequest.bucket = self.stream.bucketName;
        uploadRequest.key = key;
        uploadRequest.body = [NSURL fileURLWithPath:filePath];
        uploadRequest.ACL = AWSS3ObjectCannedACLPublicRead;
        
        [[self.transferManager upload:uploadRequest] continueWithBlock:^id(BFTask *task) {
            if (task.error) {
                [self s3RequestFailedForFileName:fileName withError:task.error];
            }
            else {
                [self s3RequestCompletedForFileName:fileName];
            }
            return nil;
        }];
    }
}

- (void)uploadLastSegment {
    NSString *lastFileName;
    NSInteger index = -1;
    NSNumberFormatter *formatter = [[NSNumberFormatter alloc] init];
    formatter.numberStyle = NSNumberFormatterDecimalStyle;
    
    // first, queue the last segment
    NSArray *contents = [[NSFileManager defaultManager] contentsOfDirectoryAtPath:self.directoryPath error:nil];
    NSUInteger tsFileCount = 0;
    for (NSString *fileName in contents) {
        if ([[fileName pathExtension] isEqualToString:@"ts"]) {
            NSArray *components = [fileName componentsSeparatedByString:@"."];
            NSString *filePrefix = [components firstObject];
            
            NSCharacterSet *nonDigitCharacterSet = [[NSCharacterSet decimalDigitCharacterSet] invertedSet];
            NSInteger currentIndex = [formatter numberFromString:[[filePrefix componentsSeparatedByCharactersInSet:nonDigitCharacterSet] componentsJoinedByString:@""]].integerValue;
            if (currentIndex > index)
                index = currentIndex;
            
            tsFileCount++;
        }
    }
    
    lastFileName = [NSString stringWithFormat:@"index%d.ts", index];
    
    NSString *uploadState = [_files objectForKey:lastFileName];
    if (!uploadState) {
        NSDictionary *segmentInfo = @{kFileNameKey: lastFileName,
                                      kFileStartDateKey: [NSDate date]};
        DDLogInfo(@"new ts file detected: %@", lastFileName);
        [_files setObject:kUploadStateQueued forKey:lastFileName];
        [_queuedSegments setObject:segmentInfo forKey:@(index)];
        [self uploadNextSegment];
    }
}

- (NSString*) awsKeyForStream:(KFS3Stream*)stream fileName:(NSString*)fileName {
    return [NSString stringWithFormat:@"%@%@", stream.awsPrefix, fileName];
}

- (void) directoryDidChange:(KFDirectoryWatcher *)folderWatcher {
    dispatch_async(_scanningQueue, ^{
        NSError *error = nil;
        NSArray *files = [[NSFileManager defaultManager] contentsOfDirectoryAtPath:_directoryPath error:&error];
        DDLogVerbose(@"Directory changed, fileCount: %lu", (unsigned long)files.count);
        if (error) {
            DDLogError(@"Error listing directory contents");
        }
        
        [self detectNewSegmentsFromFiles:files];
    });
}

- (void) detectNewSegmentsFromFiles:(NSArray*)files {
    [files enumerateObjectsUsingBlock:^(NSString *fileName, NSUInteger idx, BOOL *stop) {
        NSArray *components = [fileName componentsSeparatedByString:@"."];
        NSString *filePrefix = [components firstObject];
        NSString *fileExtension = [components lastObject];
        if ([fileExtension isEqualToString:@"ts"]) {
            NSString *uploadState = [_files objectForKey:fileName];
            if (!uploadState) {
                NSUInteger segmentIndex = [self indexForFilePrefix:filePrefix];
                NSDictionary *segmentInfo = @{kFileNameKey: fileName,
                                              kFileStartDateKey: [NSDate date]};
                DDLogInfo(@"new ts file detected: %@", fileName);
                [_files setObject:kUploadStateQueued forKey:fileName];
                [_queuedSegments setObject:segmentInfo forKey:@(segmentIndex)];
                [self uploadNextSegment];
            }
        }
    }];
}

- (NSUInteger) indexForFilePrefix:(NSString*)filePrefix {
    NSString *numbers = [filePrefix substringFromIndex:_numbersOffset];
    return [numbers integerValue];
}

- (NSURL*) urlWithFileName:(NSString*)fileName {
    NSString *key = [self awsKeyForStream:self.stream fileName:fileName];
    NSString *ssl = @"";
    if (self.useSSL) {
        ssl = @"s";
    }
    NSString *urlString = [NSString stringWithFormat:@"http%@://%@.s3.amazonaws.com/%@", ssl, self.stream.bucketName, key];
    return [NSURL URLWithString:urlString];
}

-(void)s3RequestCompletedForFileName:(NSString*)fileName
{
    dispatch_async(_scanningQueue, ^{
        if ([fileName.pathExtension isEqualToString:@"ts"]) {
            NSDictionary *segmentInfo = [_queuedSegments objectForKey:@(_nextSegmentIndexToUpload)];
            NSString *filePath = [_directoryPath stringByAppendingPathComponent:fileName];
            
            NSDate *uploadStartDate = [segmentInfo objectForKey:kFileStartDateKey];
            
            NSDate *uploadFinishDate = [NSDate date];
            
            NSError *error = nil;
            NSDictionary *fileStats = [[NSFileManager defaultManager] attributesOfItemAtPath:filePath error:&error];
            if (error) {
                DDLogError(@"Error getting stats of path %@: %@", filePath, error);
            }
            uint64_t fileSize = [fileStats fileSize];
            
            NSTimeInterval timeToUpload = [uploadFinishDate timeIntervalSinceDate:uploadStartDate];
            double bytesPerSecond = fileSize / timeToUpload;
            double KBps = bytesPerSecond / 1024;
            [_files setObject:kUploadStateFinished forKey:fileName];
            
            [[NSFileManager defaultManager] removeItemAtPath:filePath error:&error];
            if (error) {
                DDLogError(@"Error removing uploaded segment: %@", error.description);
            }
            [_queuedSegments removeObjectForKey:@(_nextSegmentIndexToUpload)];
            NSUInteger queuedSegmentsCount = _queuedSegments.count;
            if (self.isFinishedRecording && _queuedSegments.count == 0) {
                if (self.delegate && [self.delegate respondsToSelector:@selector(uploaderHasFinished:)]) {
                    [self.delegate uploaderHasFinished:self];
                }
            }
            else {
                if (self.delegate && [self.delegate respondsToSelector:@selector(uploader:didUploadSegmentAtURL:uploadSpeed:numberOfQueuedSegments:)]) {
                    NSURL *url = [self urlWithFileName:fileName];
                    dispatch_async(self.callbackQueue, ^{
                        [self.delegate uploader:self didUploadSegmentAtURL:url uploadSpeed:KBps numberOfQueuedSegments:queuedSegmentsCount];
                    });
                }
                _nextSegmentIndexToUpload++;
                [self uploadNextSegment];
            }
        }
    });
}

-(void)s3RequestFailedForFileName:(NSString*)fileName withError:(NSError *)error
{
    dispatch_async(_scanningQueue, ^{
        [_files setObject:kUploadStateFailed forKey:fileName];
        DDLogError(@"Failed to upload request, requeuing %@: %@", fileName, error.description);
        [self uploadNextSegment];
    });
}

@end
