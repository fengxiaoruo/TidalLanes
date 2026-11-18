#import <AppKit/AppKit.h>
#import <AVFoundation/AVFoundation.h>
#import <CoreMedia/CoreMedia.h>
#import <CoreText/CoreText.h>
#import <Foundation/Foundation.h>
#import <QuartzCore/QuartzCore.h>

static void Fail(NSString *message) {
    fprintf(stderr, "%s\n", message.UTF8String);
    exit(1);
}

static void RemoveIfPresent(NSURL *url) {
    if ([NSFileManager.defaultManager fileExistsAtPath:url.path]) {
        NSError *error = nil;
        if (![NSFileManager.defaultManager removeItemAtURL:url error:&error]) {
            Fail([NSString stringWithFormat:@"Could not replace %@: %@",
                url.lastPathComponent, error.localizedDescription]);
        }
    }
}

static void ExportAsset(AVAsset *asset,
                        NSString *preset,
                        NSURL *outputURL,
                        AVVideoComposition *videoComposition) {
    RemoveIfPresent(outputURL);
    AVAssetExportSession *exporter = [[AVAssetExportSession alloc] initWithAsset:asset
                                                                      presetName:preset];
    if (!exporter) {
        Fail([NSString stringWithFormat:@"Could not create export session for %@",
            outputURL.lastPathComponent]);
    }
    exporter.outputURL = outputURL;
    exporter.outputFileType = AVFileTypeQuickTimeMovie;
    exporter.videoComposition = videoComposition;
    exporter.shouldOptimizeForNetworkUse = YES;

    dispatch_semaphore_t finished = dispatch_semaphore_create(0);
    [exporter exportAsynchronouslyWithCompletionHandler:^{
        dispatch_semaphore_signal(finished);
    }];
    dispatch_semaphore_wait(finished, DISPATCH_TIME_FOREVER);
    if (exporter.status != AVAssetExportSessionStatusCompleted) {
        Fail([NSString stringWithFormat:@"Export failed for %@: %@",
            outputURL.lastPathComponent,
            exporter.error.localizedDescription ?: @"unknown error"]);
    }
}

static void WriteCaptionedHEVC(AVAsset *asset,
                               AVVideoComposition *videoComposition,
                               NSURL *outputURL,
                               CGSize renderSize,
                               float frameRate,
                               NSInteger bitRate) {
    RemoveIfPresent(outputURL);
    NSError *error = nil;
    AVAssetReader *reader = [[AVAssetReader alloc] initWithAsset:asset error:&error];
    if (!reader) {
        Fail([NSString stringWithFormat:@"Could not create caption reader: %@",
            error.localizedDescription]);
    }
    NSArray<AVAssetTrack *> *videoTracks = [asset tracksWithMediaType:AVMediaTypeVideo];
    NSDictionary *readerSettings = @{
        (id)kCVPixelBufferPixelFormatTypeKey: @(kCVPixelFormatType_420YpCbCr8BiPlanarVideoRange)
    };
    AVAssetReaderVideoCompositionOutput *readerOutput =
        [[AVAssetReaderVideoCompositionOutput alloc] initWithVideoTracks:videoTracks
                                                            videoSettings:readerSettings];
    readerOutput.videoComposition = videoComposition;
    readerOutput.alwaysCopiesSampleData = NO;
    if (![reader canAddOutput:readerOutput]) {
        Fail(@"Could not attach the caption reader output");
    }
    [reader addOutput:readerOutput];

    AVAssetWriter *writer = [[AVAssetWriter alloc] initWithURL:outputURL
                                                      fileType:AVFileTypeQuickTimeMovie
                                                         error:&error];
    if (!writer) {
        Fail([NSString stringWithFormat:@"Could not create caption writer: %@",
            error.localizedDescription]);
    }
    NSDictionary *compression = @{
        AVVideoAverageBitRateKey: @(bitRate),
        AVVideoExpectedSourceFrameRateKey: @(frameRate),
        AVVideoMaxKeyFrameIntervalKey: @(MAX(1, lround(frameRate * 2))),
        AVVideoAllowFrameReorderingKey: @YES
    };
    NSDictionary *writerSettings = @{
        AVVideoCodecKey: AVVideoCodecTypeHEVC,
        AVVideoWidthKey: @((NSInteger)lround(renderSize.width)),
        AVVideoHeightKey: @((NSInteger)lround(renderSize.height)),
        AVVideoCompressionPropertiesKey: compression,
        AVVideoColorPropertiesKey: @{
            AVVideoColorPrimariesKey: AVVideoColorPrimaries_ITU_R_709_2,
            AVVideoTransferFunctionKey: AVVideoTransferFunction_ITU_R_709_2,
            AVVideoYCbCrMatrixKey: AVVideoYCbCrMatrix_ITU_R_709_2
        }
    };
    AVAssetWriterInput *writerInput = [AVAssetWriterInput assetWriterInputWithMediaType:AVMediaTypeVideo
                                                                          outputSettings:writerSettings];
    writerInput.expectsMediaDataInRealTime = NO;
    if (![writer canAddInput:writerInput]) {
        Fail(@"Could not attach the HEVC writer input");
    }
    [writer addInput:writerInput];

    if (![writer startWriting]) {
        Fail([NSString stringWithFormat:@"Could not start the HEVC writer: %@",
            writer.error.localizedDescription]);
    }
    if (![reader startReading]) {
        Fail([NSString stringWithFormat:@"Could not start the caption reader: %@",
            reader.error.localizedDescription]);
    }
    [writer startSessionAtSourceTime:kCMTimeZero];

    dispatch_semaphore_t finished = dispatch_semaphore_create(0);
    dispatch_queue_t queue = dispatch_queue_create("tidallanes.caption.encode", DISPATCH_QUEUE_SERIAL);
    __block BOOL appendFailed = NO;
    [writerInput requestMediaDataWhenReadyOnQueue:queue usingBlock:^{
        while (writerInput.readyForMoreMediaData) {
            @autoreleasepool {
                CMSampleBufferRef sample = [readerOutput copyNextSampleBuffer];
                if (sample) {
                    if (![writerInput appendSampleBuffer:sample]) {
                        appendFailed = YES;
                        CFRelease(sample);
                        [reader cancelReading];
                        [writerInput markAsFinished];
                        [writer cancelWriting];
                        dispatch_semaphore_signal(finished);
                        return;
                    }
                    CFRelease(sample);
                } else {
                    [writerInput markAsFinished];
                    [writer finishWritingWithCompletionHandler:^{
                        dispatch_semaphore_signal(finished);
                    }];
                    return;
                }
            }
        }
    }];
    dispatch_semaphore_wait(finished, DISPATCH_TIME_FOREVER);

    if (appendFailed || reader.status == AVAssetReaderStatusFailed ||
        writer.status != AVAssetWriterStatusCompleted) {
        Fail([NSString stringWithFormat:@"Custom HEVC export failed: %@ %@",
            reader.error.localizedDescription ?: @"",
            writer.error.localizedDescription ?: @""]);
    }
}

static CGImageRef CreateCaptionLineImage(NSString *text, CGSize size, CGFloat fontSize) {
    const CGFloat scale = 2.0;
    size_t pixelWidth = (size_t)ceil(size.width * scale);
    size_t pixelHeight = (size_t)ceil(size.height * scale);
    CGColorSpaceRef colorSpace = CGColorSpaceCreateDeviceRGB();
    CGContextRef context = CGBitmapContextCreate(NULL,
                                                 pixelWidth,
                                                 pixelHeight,
                                                 8,
                                                 0,
                                                 colorSpace,
                                                 (CGBitmapInfo)kCGImageAlphaPremultipliedLast);
    CGColorSpaceRelease(colorSpace);
    if (!context) {
        return NULL;
    }
    CGContextScaleCTM(context, scale, scale);
    CGContextSetTextMatrix(context, CGAffineTransformIdentity);

    CTFontRef font = CTFontCreateWithName(CFSTR("HelveticaNeue-Medium"), fontSize, NULL);
    CGColorRef white = CGColorCreateGenericRGB(1, 1, 1, 1);
    NSDictionary *attributes = @{
        (__bridge id)kCTFontAttributeName: (__bridge id)font,
        (__bridge id)kCTForegroundColorAttributeName: (__bridge id)white
    };
    NSAttributedString *attributed = [[NSAttributedString alloc] initWithString:text
                                                                    attributes:attributes];
    CTLineRef line = CTLineCreateWithAttributedString((__bridge CFAttributedStringRef)attributed);
    CGFloat ascent = 0;
    CGFloat descent = 0;
    CGFloat leading = 0;
    double lineWidth = CTLineGetTypographicBounds(line, &ascent, &descent, &leading);
    CGFloat x = MAX(0, (size.width - lineWidth) / 2.0);
    CGFloat baselineY = (size.height - ascent - descent) / 2.0 + descent;
    CGContextSetTextPosition(context, x, baselineY);
    CTLineDraw(line, context);
    CGImageRef image = CGBitmapContextCreateImage(context);

    CFRelease(line);
    CGColorRelease(white);
    CFRelease(font);
    CGContextRelease(context);
    return image;
}

static AVMutableComposition *VideoOnlyComposition(AVAsset *sourceAsset,
                                                   AVAssetTrack *sourceVideoTrack) {
    AVMutableComposition *composition = [AVMutableComposition composition];
    AVMutableCompositionTrack *videoTrack = [composition addMutableTrackWithMediaType:AVMediaTypeVideo
                                                                      preferredTrackID:kCMPersistentTrackID_Invalid];
    NSError *error = nil;
    BOOL inserted = [videoTrack insertTimeRange:CMTimeRangeMake(kCMTimeZero, sourceAsset.duration)
                                         ofTrack:sourceVideoTrack
                                          atTime:kCMTimeZero
                                           error:&error];
    if (!inserted) {
        Fail([NSString stringWithFormat:@"Could not insert source video: %@",
            error.localizedDescription]);
    }
    return composition;
}

int main(void) {
    @autoreleasepool {
        NSString *root = NSFileManager.defaultManager.currentDirectoryPath;
        NSString *preSlides = [root stringByAppendingPathComponent:@"Documents/PreSlides"];
        NSString *voiceDir = [preSlides stringByAppendingPathComponent:@".tidal_en_tmp"];
        NSURL *sourceURL = [NSURL fileURLWithPath:[preSlides stringByAppendingPathComponent:@"TIdalVedio.mov"]];
        NSURL *mutedURL = [NSURL fileURLWithPath:[preSlides stringByAppendingPathComponent:@"TIdalVedio_muted.mov"]];
        NSURL *subtitlesURL = [NSURL fileURLWithPath:[preSlides stringByAppendingPathComponent:@"TIdalVedio_EN_subtitles.mov"]];
        NSURL *voiceoverURL = [NSURL fileURLWithPath:[preSlides stringByAppendingPathComponent:@"TIdalVedio_EN_subtitles_voiceover.mov"]];

        NSArray<NSDictionary *> *segments = @[
            @{
                @"start": @1.0,
                @"end": @5.0,
                @"caption": @[
                    @"Before opening, the left-side outbound lanes are congested;",
                    @"the right-side inbound lanes remain uncongested."
                ],
                @"voice": @"segment1.m4a"
            },
            @{
                @"start": @5.0,
                @"end": @16.0,
                @"caption": @[
                    @"An automated barrier-transfer vehicle reallocates one lane from right to left.",
                    @"Left: +1 lane   |   Right: -1 lane"
                ],
                @"voice": @"segment2.m4a"
            },
            @{
                @"start": @16.0,
                @"end": @24.0,
                @"caption": @[
                    @"A simultaneous view from another angle captures the lane just after opening.",
                    @"Vehicles immediately enter the reallocated lane from the congested direction."
                ],
                @"voice": @"segment3.m4a"
            },
            @{
                @"start": @24.0,
                @"end": @29.767,
                @"caption": @[
                    @"Reallocation complete: traffic flow gradually stabilizes."
                ],
                @"voice": @"segment4.m4a"
            }
        ];

        AVURLAsset *sourceAsset = [AVURLAsset URLAssetWithURL:sourceURL options:nil];
        AVAssetTrack *sourceVideoTrack = [sourceAsset tracksWithMediaType:AVMediaTypeVideo].firstObject;
        if (!sourceVideoTrack) {
            Fail(@"The new MOV file has no video track");
        }
        printf("Source: %.3f seconds, %.3f fps, %dx%d\n",
               CMTimeGetSeconds(sourceAsset.duration),
               sourceVideoTrack.nominalFrameRate,
               (int)sourceVideoTrack.naturalSize.width,
               (int)sourceVideoTrack.naturalSize.height);

        // Version 1: remove all audio while passing the original HEVC video packets through.
        if (![NSFileManager.defaultManager fileExistsAtPath:mutedURL.path]) {
            AVMutableComposition *mutedComposition = VideoOnlyComposition(sourceAsset, sourceVideoTrack);
            ExportAsset(mutedComposition, AVAssetExportPresetPassthrough, mutedURL, nil);
            printf("Created %s\n", mutedURL.path.UTF8String);
        } else {
            printf("Kept %s\n", mutedURL.path.UTF8String);
        }

        // Version 2: burn English captions into a new HEVC stream at source dimensions and frame rate.
        AVMutableComposition *captionComposition = VideoOnlyComposition(sourceAsset, sourceVideoTrack);
        AVMutableCompositionTrack *captionVideoTrack = [captionComposition tracksWithMediaType:AVMediaTypeVideo].firstObject;

        CGSize transformedSize = CGSizeApplyAffineTransform(sourceVideoTrack.naturalSize,
                                                             sourceVideoTrack.preferredTransform);
        CGSize renderSize = CGSizeMake(fabs(transformedSize.width), fabs(transformedSize.height));

        AVMutableVideoCompositionInstruction *instruction = [AVMutableVideoCompositionInstruction videoCompositionInstruction];
        instruction.timeRange = CMTimeRangeMake(kCMTimeZero, sourceAsset.duration);
        AVMutableVideoCompositionLayerInstruction *layerInstruction =
            [AVMutableVideoCompositionLayerInstruction videoCompositionLayerInstructionWithAssetTrack:captionVideoTrack];
        [layerInstruction setTransform:sourceVideoTrack.preferredTransform atTime:kCMTimeZero];
        instruction.layerInstructions = @[layerInstruction];

        AVMutableVideoComposition *videoComposition = [AVMutableVideoComposition videoComposition];
        videoComposition.instructions = @[instruction];
        videoComposition.renderSize = renderSize;
        if (CMTIME_IS_VALID(sourceVideoTrack.minFrameDuration) &&
            !CMTIME_IS_INDEFINITE(sourceVideoTrack.minFrameDuration) &&
            CMTimeCompare(sourceVideoTrack.minFrameDuration, kCMTimeZero) > 0) {
            videoComposition.frameDuration = sourceVideoTrack.minFrameDuration;
        } else {
            int32_t fps = sourceVideoTrack.nominalFrameRate > 0
                ? (int32_t)lround(sourceVideoTrack.nominalFrameRate)
                : 30;
            videoComposition.frameDuration = CMTimeMake(1, fps);
        }

        CALayer *parentLayer = [CALayer layer];
        parentLayer.frame = CGRectMake(0, 0, renderSize.width, renderSize.height);
        CALayer *videoLayer = [CALayer layer];
        videoLayer.frame = parentLayer.frame;
        [parentLayer addSublayer:videoLayer];

        double fullDuration = CMTimeGetSeconds(sourceAsset.duration);
        for (NSDictionary *segment in segments) {
            double start = [segment[@"start"] doubleValue];
            double end = MIN([segment[@"end"] doubleValue], fullDuration);
            NSArray<NSString *> *captionLines = segment[@"caption"];

            CALayer *panel = [CALayer layer];
            panel.frame = CGRectMake(50, 38, renderSize.width - 100, 170);
            panel.backgroundColor = [NSColor colorWithWhite:0 alpha:0.72].CGColor;
            panel.cornerRadius = 20;
            panel.masksToBounds = YES;
            panel.opacity = 0;

            CGFloat lineHeight = 58;
            CGFloat blockHeight = lineHeight * captionLines.count;
            CGFloat lowerMargin = (panel.bounds.size.height - blockHeight) / 2.0;
            [captionLines enumerateObjectsUsingBlock:^(NSString *line, NSUInteger index, BOOL *stop) {
                CGImageRef captionImage = CreateCaptionLineImage(line,
                    CGSizeMake(panel.bounds.size.width, lineHeight), 40);
                if (!captionImage) {
                    Fail(@"Could not render an English caption");
                }
                CALayer *textLayer = [CALayer layer];
                CGFloat y = panel.bounds.size.height - lowerMargin - lineHeight * (index + 1);
                textLayer.frame = CGRectMake(0, y, panel.bounds.size.width, lineHeight);
                textLayer.contents = (__bridge id)captionImage;
                textLayer.contentsGravity = kCAGravityResizeAspect;
                [panel addSublayer:textLayer];
                CGImageRelease(captionImage);
            }];

            CAKeyframeAnimation *fade = [CAKeyframeAnimation animationWithKeyPath:@"opacity"];
            double fadeSeconds = 0.12;
            fade.values = @[@0, @0, @1, @1, @0, @0];
            fade.keyTimes = @[
                @0,
                @(start / fullDuration),
                @(MIN(start + fadeSeconds, end) / fullDuration),
                @(MAX(end - fadeSeconds, start) / fullDuration),
                @(end / fullDuration),
                @1
            ];
            fade.duration = fullDuration;
            fade.beginTime = AVCoreAnimationBeginTimeAtZero;
            fade.removedOnCompletion = NO;
            fade.fillMode = kCAFillModeBoth;
            [panel addAnimation:fade forKey:@"captionVisibility"];
            [parentLayer addSublayer:panel];
        }

        videoComposition.animationTool =
            [AVVideoCompositionCoreAnimationTool videoCompositionCoreAnimationToolWithPostProcessingAsVideoLayer:videoLayer
                                                                                                     inLayer:parentLayer];
        WriteCaptionedHEVC(captionComposition,
                           videoComposition,
                           subtitlesURL,
                           renderSize,
                           sourceVideoTrack.nominalFrameRate > 0 ? sourceVideoTrack.nominalFrameRate : 30,
                           16000000);
        printf("Created %s\n", subtitlesURL.path.UTF8String);

        // Version 3: pass the captioned HEVC stream through and add AAC narration only.
        AVURLAsset *captionedAsset = [AVURLAsset URLAssetWithURL:subtitlesURL options:nil];
        AVAssetTrack *captionedVideoTrack = [captionedAsset tracksWithMediaType:AVMediaTypeVideo].firstObject;
        if (!captionedVideoTrack) {
            Fail(@"Captioned file has no video track");
        }
        AVMutableComposition *voiceoverComposition = [AVMutableComposition composition];
        AVMutableCompositionTrack *finalVideoTrack =
            [voiceoverComposition addMutableTrackWithMediaType:AVMediaTypeVideo
                                               preferredTrackID:kCMPersistentTrackID_Invalid];
        NSError *error = nil;
        BOOL inserted = [finalVideoTrack insertTimeRange:CMTimeRangeMake(kCMTimeZero, captionedAsset.duration)
                                                  ofTrack:captionedVideoTrack
                                                   atTime:kCMTimeZero
                                                    error:&error];
        if (!inserted) {
            Fail([NSString stringWithFormat:@"Could not insert captioned video: %@",
                error.localizedDescription]);
        }

        AVMutableCompositionTrack *narrationTrack =
            [voiceoverComposition addMutableTrackWithMediaType:AVMediaTypeAudio
                                               preferredTrackID:kCMPersistentTrackID_Invalid];
        for (NSDictionary *segment in segments) {
            double start = [segment[@"start"] doubleValue];
            double end = MIN([segment[@"end"] doubleValue], fullDuration);
            NSString *voicePath = [voiceDir stringByAppendingPathComponent:segment[@"voice"]];
            AVURLAsset *voiceAsset = [AVURLAsset URLAssetWithURL:[NSURL fileURLWithPath:voicePath] options:nil];
            AVAssetTrack *voiceTrack = [voiceAsset tracksWithMediaType:AVMediaTypeAudio].firstObject;
            if (!voiceTrack) {
                Fail([NSString stringWithFormat:@"Narration file has no audio track: %@",
                    segment[@"voice"]]);
            }
            CMTime insertionTime = CMTimeMakeWithSeconds(start + 0.15, 600);
            CMTime availableDuration = CMTimeMakeWithSeconds(MAX(0, end - start - 0.25), 600);
            CMTime voiceDuration = CMTimeCompare(voiceAsset.duration, availableDuration) < 0
                ? voiceAsset.duration
                : availableDuration;
            error = nil;
            inserted = [narrationTrack insertTimeRange:CMTimeRangeMake(kCMTimeZero, voiceDuration)
                                                ofTrack:voiceTrack
                                                 atTime:insertionTime
                                                  error:&error];
            if (!inserted) {
                Fail([NSString stringWithFormat:@"Could not insert narration %@: %@",
                    segment[@"voice"], error.localizedDescription]);
            }
        }

        ExportAsset(voiceoverComposition,
                    AVAssetExportPresetPassthrough,
                    voiceoverURL,
                    nil);
        printf("Created %s\n", voiceoverURL.path.UTF8String);
    }
    return 0;
}
