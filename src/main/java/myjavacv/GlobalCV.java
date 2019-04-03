package myjavacv;

import org.bytedeco.javacpp.FloatPointer;
import org.bytedeco.javacpp.IntPointer;
import org.bytedeco.javacpp.PointerPointer;
import org.bytedeco.javacpp.opencv_core.*;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_imgproc.*;

public class GlobalCV {

    public static Mat getHSVHistogram(Mat img) {
        // Transfer to Mat of HSV
        cvtColor(img, img, CV_BGR2HSV);

        // Get histogram and normalize
        Mat hist = new Mat();

        int h_bins = 50;
        int s_bins = 60;
        IntPointer histSize = new IntPointer(h_bins, s_bins);

        FloatPointer h_ranges = new FloatPointer(0, 180);
        FloatPointer s_ranges = new FloatPointer(0, 256);
        PointerPointer ranges = new PointerPointer(h_ranges, s_ranges);

        IntPointer channels = new IntPointer(0, 1);
        Mat mask = new Mat();

        calcHist(img, 1, channels, mask, hist, 2, histSize, ranges, true, false);
        normalize(hist, hist, 0, 1, NORM_MINMAX, -1, new Mat());

        return hist;
    }

}
