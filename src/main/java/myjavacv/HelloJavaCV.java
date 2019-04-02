package myjavacv;

import org.bytedeco.javacpp.opencv_core.*;
import org.bytedeco.javacpp.opencv_highgui.*;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.Java2DFrameConverter;
import org.bytedeco.javacv.OpenCVFrameConverter;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.IOException;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_highgui.*;
import static org.bytedeco.javacpp.opencv_imgproc.*;

public class HelloJavaCV {

    private static void analyseHSV(Frame frame) {

        // Convert Frame to IplImage
        OpenCVFrameConverter.ToIplImage converterToIplImage = new OpenCVFrameConverter.ToIplImage();
        final IplImage src = converterToIplImage.convert(frame);
        CvSize size = src.cvSize();
        int depth = src.depth();

        // If not colored
//        if(src == null || src.nChannels() < 3) {
//            new Exception("Error!");
//        }

        // Show image
        //cvNamedWindow("Image",CV_WINDOW_AUTOSIZE);

        // Create empty image (placeholder)
        final IplImage hsvImage = cvCreateImage(size, depth, 3);

        // Convert "src" (BGR) to "HSV" form, get it in hsv
        cvCvtColor(src, hsvImage, CV_BGR2HSV);

        // Create empty image channel
        IplImage hChannel = cvCreateImage(size, depth, 1);
        IplImage sChannel = cvCreateImage(size, depth, 1);
        IplImage vChannel = cvCreateImage(size, depth, 1);

        // Split image to H S V Channel
        cvSplit(hsvImage, hChannel, sChannel, vChannel, null);

    }

    public static void main(String[] args) throws IOException {

        // History Dump File delete
        File dumpDir = new File("frame-dump");
        if (dumpDir.exists()) {
            Global.deleteDir(dumpDir);
        }
        dumpDir.mkdirs();

        FFmpegFrameGrabber frameGrabber = new FFmpegFrameGrabber("EP00.mp4");
        Java2DFrameConverter converter = new Java2DFrameConverter();

        frameGrabber.start();
        frameGrabber.setTimestamp(25000000); // 微秒

        while (true) {
            Frame frame = frameGrabber.grabImage();
//            Frame frame = frameGrabber.grabKeyFrame();
            if (frame == null) {
                break;
            }

//            analyseHSV(frame);

            BufferedImage image = converter.convert(frame);

            if (image == null) {
                continue;
            }
            ImageIO.write(image, "png", new File("frame-dump/video-frame-" + frameGrabber.getTimestamp() + ".png"));
            if (frameGrabber.getTimestamp() >= 35000000) {  // 微秒
                break;
            }
        }

        frameGrabber.stop();

    }

}
