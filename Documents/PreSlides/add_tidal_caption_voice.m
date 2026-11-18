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

static CGImageRef CreateCaptionLineImage(NSString *text, CGSize size) {
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

    CTFontRef font = CTFontCreateWithName(CFSTR("PingFangSC-Semibold"), 30, NULL);
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

int main(int argc, const char *argv[]) {
    @autoreleasepool {
        NSString *root = NSFileManager.defaultManager.currentDirectoryPath;
        NSString *preSlides = [root stringByAppendingPathComponent:@"Documents/PreSlides"];
        NSURL *inputURL = [NSURL fileURLWithPath:[preSlides stringByAppendingPathComponent:@"TIdalVedio.mp4"]];
        NSURL *outputURL = [NSURL fileURLWithPath:[preSlides stringByAppendingPathComponent:@"TIdalVedio_captioned_zh.mp4"]];
        NSString *voiceDir = [preSlides stringByAppendingPathComponent:@".tidal_voice_tmp"];

        NSArray<NSDictionary *> *segments = @[
            @{
                @"start": @1.0,
                @"end": @5.0,
                @"caption": @"潮汐车道实施前：左侧出城方向拥堵\n右侧进城方向通畅",
                @"voice": @"segment1.aiff"
            },
            @{
                @"start": @5.0,
                @"end": @16.0,
                @"caption": @"潮汐车道正在实施：自动作业车完成车道重分配\n左侧增加 1 条车道，右侧减少 1 条车道",
                @"voice": @"segment2.aiff"
            },
            @{
                @"start": @16.0,
                @"end": @24.0,
                @"caption": @"车道调整需要时间。同期另一视角显示：车道刚开放\n车辆立即从拥堵方向驶入新分配车道",
                @"voice": @"segment3.aiff"
            },
            @{
                @"start": @24.0,
                @"end": @29.8,
                @"caption": @"车道重分配完成\n车流逐步趋于稳定",
                @"voice": @"segment4.aiff"
            }
        ];

        AVURLAsset *sourceAsset = [AVURLAsset URLAssetWithURL:inputURL options:nil];
        AVAssetTrack *sourceVideoTrack = [sourceAsset tracksWithMediaType:AVMediaTypeVideo].firstObject;
        if (!sourceVideoTrack) {
            Fail(@"Input video track not found");
        }

        AVMutableComposition *composition = [AVMutableComposition composition];
        AVMutableCompositionTrack *videoTrack = [composition addMutableTrackWithMediaType:AVMediaTypeVideo
                                                                         preferredTrackID:kCMPersistentTrackID_Invalid];
        NSError *error = nil;
        BOOL inserted = [videoTrack insertTimeRange:CMTimeRangeMake(kCMTimeZero, sourceAsset.duration)
                                             ofTrack:sourceVideoTrack
                                              atTime:kCMTimeZero
                                               error:&error];
        if (!inserted) {
            Fail([NSString stringWithFormat:@"Could not insert input video: %@", error.localizedDescription]);
        }

        // Deliberately omit the source audio. The output contains narration only.
        AVMutableCompositionTrack *narrationTrack = [composition addMutableTrackWithMediaType:AVMediaTypeAudio
                                                                             preferredTrackID:kCMPersistentTrackID_Invalid];

        for (NSDictionary *segment in segments) {
            double start = [segment[@"start"] doubleValue];
            double end = [segment[@"end"] doubleValue];
            NSString *voicePath = [voiceDir stringByAppendingPathComponent:segment[@"voice"]];
            AVURLAsset *voiceAsset = [AVURLAsset URLAssetWithURL:[NSURL fileURLWithPath:voicePath] options:nil];
            AVAssetTrack *voiceTrack = [voiceAsset tracksWithMediaType:AVMediaTypeAudio].firstObject;
            if (!voiceTrack) {
                Fail([NSString stringWithFormat:@"Narration file has no audio track: %@", segment[@"voice"]]);
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

        CGSize transformedSize = CGSizeApplyAffineTransform(sourceVideoTrack.naturalSize,
                                                             sourceVideoTrack.preferredTransform);
        CGSize renderSize = CGSizeMake(fabs(transformedSize.width), fabs(transformedSize.height));

        AVMutableVideoCompositionInstruction *instruction = [AVMutableVideoCompositionInstruction videoCompositionInstruction];
        instruction.timeRange = CMTimeRangeMake(kCMTimeZero, sourceAsset.duration);
        AVMutableVideoCompositionLayerInstruction *layerInstruction =
            [AVMutableVideoCompositionLayerInstruction videoCompositionLayerInstructionWithAssetTrack:videoTrack];
        [layerInstruction setTransform:sourceVideoTrack.preferredTransform atTime:kCMTimeZero];
        instruction.layerInstructions = @[layerInstruction];

        AVMutableVideoComposition *videoComposition = [AVMutableVideoComposition videoComposition];
        videoComposition.instructions = @[instruction];
        videoComposition.renderSize = renderSize;
        videoComposition.frameDuration = CMTimeMake(1, 30);

        CALayer *parentLayer = [CALayer layer];
        parentLayer.frame = CGRectMake(0, 0, renderSize.width, renderSize.height);
        CALayer *videoLayer = [CALayer layer];
        videoLayer.frame = parentLayer.frame;
        [parentLayer addSublayer:videoLayer];

        double fullDuration = CMTimeGetSeconds(sourceAsset.duration);
        for (NSDictionary *segment in segments) {
            double start = [segment[@"start"] doubleValue];
            double end = [segment[@"end"] doubleValue];

            CALayer *panel = [CALayer layer];
            panel.frame = CGRectMake(34, 26, renderSize.width - 68, 112);
            panel.backgroundColor = [NSColor colorWithWhite:0 alpha:0.72].CGColor;
            panel.cornerRadius = 14;
            panel.masksToBounds = YES;
            panel.opacity = 0;

            NSArray<NSString *> *captionLines = [segment[@"caption"] componentsSeparatedByString:@"\n"];
            CGFloat lineHeight = 42;
            CGFloat blockHeight = lineHeight * captionLines.count;
            CGFloat lowerMargin = (panel.bounds.size.height - blockHeight) / 2.0;
            [captionLines enumerateObjectsUsingBlock:^(NSString *line, NSUInteger index, BOOL *stop) {
                CGImageRef captionImage = CreateCaptionLineImage(line,
                    CGSizeMake(panel.bounds.size.width, lineHeight));
                if (!captionImage) {
                    Fail(@"Could not render caption image");
                }
                CALayer *textLayer = [CALayer layer];
                CGFloat y = panel.bounds.size.height - lowerMargin - lineHeight * (index + 1);
                textLayer.frame = CGRectMake(0, y, panel.bounds.size.width, lineHeight);
                textLayer.contents = (__bridge id)captionImage;
                textLayer.contentsGravity = kCAGravityResizeAspect;
                [panel addSublayer:textLayer];
                CGImageRelease(captionImage);
            }];

            double fadeSeconds = 0.12;
            CAKeyframeAnimation *fade = [CAKeyframeAnimation animationWithKeyPath:@"opacity"];
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

        if ([NSFileManager.defaultManager fileExistsAtPath:outputURL.path]) {
            [NSFileManager.defaultManager removeItemAtURL:outputURL error:nil];
        }

        AVAssetExportSession *exporter = [[AVAssetExportSession alloc] initWithAsset:composition
                                                                          presetName:AVAssetExportPresetHighestQuality];
        if (!exporter) {
            Fail(@"Could not create export session");
        }
        exporter.outputURL = outputURL;
        exporter.outputFileType = AVFileTypeMPEG4;
        exporter.videoComposition = videoComposition;
        exporter.shouldOptimizeForNetworkUse = YES;

        dispatch_semaphore_t finished = dispatch_semaphore_create(0);
        [exporter exportAsynchronouslyWithCompletionHandler:^{
            dispatch_semaphore_signal(finished);
        }];
        dispatch_semaphore_wait(finished, DISPATCH_TIME_FOREVER);

        if (exporter.status != AVAssetExportSessionStatusCompleted) {
            Fail([NSString stringWithFormat:@"Export failed: %@",
                exporter.error.localizedDescription ?: @"unknown error"]);
        }

        printf("%s\n", outputURL.path.UTF8String);
    }
    return 0;
}
