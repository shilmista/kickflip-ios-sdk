//
//  KFHLSMonitor.h
//  FFmpegEncoder
//
//  Created by Christopher Ballinger on 1/16/14.
//  Copyright (c) 2014 Christopher Ballinger. All rights reserved.
//

#import <Foundation/Foundation.h>
#import "KFHLSUploader.h"

@class KFHLSMonitor;

@protocol KFHLSMonitorDelegate <NSObject>
- (void)monitor:(KFHLSMonitor *)monitor didFinishUploading:(id)sender;
@end

static NSString * const kKFHLSMonitorFinishedUploadingSegmentsNotification = @"kKFHLSFinishedUploadingSegments";

@interface KFHLSMonitor : NSObject <KFHLSUploaderDelegate>

@property (nonatomic, weak) id <KFHLSMonitorDelegate> delegate;

+ (KFHLSMonitor*) sharedMonitor;

- (void) startMonitoringFolderPath:(NSString*)path endpoint:(KFS3Stream*)endpoint delegate:(id<KFHLSUploaderDelegate>)delegate;
- (void) finishUploadingContentsAtFolderPath:(NSString*)path endpoint:(KFS3Stream*)endpoint; //reclaims delegate of uploader

@end
