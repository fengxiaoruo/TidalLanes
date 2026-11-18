#import <AppKit/AppKit.h>
#import <AVFoundation/AVFoundation.h>
#import <Foundation/Foundation.h>

int main(void) {
    @autoreleasepool {
        NSString *root = NSFileManager.defaultManager.currentDirectoryPath;
        NSString *dir = [root stringByAppendingPathComponent:@"Documents/PreSlides/.tidal_en_tmp"];
        NSURL *videoURL = [NSURL fileURLWithPath:[root stringByAppendingPathComponent:@"Documents/PreSlides/TIdalVedio_EN_subtitles_voiceover.mov"]];
        AVURLAsset *asset = [AVURLAsset URLAssetWithURL:videoURL options:nil];
        AVAssetImageGenerator *generator = [AVAssetImageGenerator assetImageGeneratorWithAsset:asset];
        generator.appliesPreferredTrackTransform = YES;
        generator.requestedTimeToleranceBefore = kCMTimeZero;
        generator.requestedTimeToleranceAfter = kCMTimeZero;

        for (NSNumber *second in @[@0.5, @2, @10, @20, @26]) {
            NSError *error = nil;
            CGImageRef image = [generator copyCGImageAtTime:CMTimeMakeWithSeconds(second.doubleValue, 600)
                                                actualTime:NULL
                                                     error:&error];
            if (!image) {
                fprintf(stderr, "Frame extraction failed: %s\n", error.localizedDescription.UTF8String);
                return 1;
            }
            NSBitmapImageRep *bitmap = [[NSBitmapImageRep alloc] initWithCGImage:image];
            NSData *png = [bitmap representationUsingType:NSBitmapImageFileTypePNG properties:@{}];
            NSString *label = [NSString stringWithFormat:@"%@", second];
            NSString *path = [dir stringByAppendingPathComponent:[NSString stringWithFormat:@"qa_%@s.png", label]];
            [png writeToFile:path atomically:YES];
            CGImageRelease(image);
            printf("%s\n", path.UTF8String);
        }
    }
    return 0;
}
