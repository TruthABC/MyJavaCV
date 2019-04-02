package myjavacv.spark;

import myjavacv.Global;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bytedeco.javacpp.FloatPointer;
import org.bytedeco.javacpp.IntPointer;
import org.bytedeco.javacpp.PointerPointer;
import org.bytedeco.javacpp.opencv_core.*;
import org.bytedeco.javacv.*;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_imgcodecs.imread;
import static org.bytedeco.javacpp.opencv_imgproc.*;

public class VideoSearcher implements Serializable {

    private final long SEC = 1000000;
    private final long INTERVAL = 30 * SEC;

    private final String DUMP_PATH_1 = "search_image_by_video";

    private String mImagePath = "winner2.png";
    private String mVideoPath = "EP00.mp4";

    public VideoSearcher() {}

    public void initializeImage(String imagePath) {
        mImagePath = imagePath;
    }

    public void initializeVideo(String videoPath) {
        mVideoPath = videoPath;
    }

    public void searchImageByVideo() throws FrameGrabber.Exception {
        // History Dump File delete
        File dumpDir = new File(DUMP_PATH_1);
        if (dumpDir.exists()) {
            Global.deleteDir(dumpDir);
        }
        dumpDir.mkdirs();

        // Init to get video length
        FFmpegFrameGrabber grabber4Init = new FFmpegFrameGrabber(mVideoPath);
        grabber4Init.start();
        long length = grabber4Init.getLengthInTime();
        grabber4Init.stop();

        // Arrange start timestamps
        List<Long> startTimeStamps = new ArrayList<>();
        for (long i = 0;  i < length; i += INTERVAL) {
            startTimeStamps.add(i);
        }

        // Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName(DUMP_PATH_1);

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Paralleling list to get RDD
        JavaRDD<Long> startTimeStampRDD = sc.parallelize(startTimeStamps);

        // Run Tasks of Searching
        JavaPairRDD<Long, Double> similarityRDD = startTimeStampRDD.mapToPair((startTime)->{
            System.out.println("Task [" + startTime + " to " + Math.min(startTime + INTERVAL, length) + ")");
            double maxSimilarity = 0.0;
            long maxSimTime = startTime;

            FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(mVideoPath);
            grabber.start();
            if (startTime != 0) {
                grabber.setTimestamp(startTime);
            }

            while (true) {
                Frame frame = grabber.grabImage();
                if (frame == null) {
                    break;
                }

                // Dump image for debug
                long timeNow = grabber.getTimestamp();
                ImageIO.write(new Java2DFrameConverter().convert(frame) , "png", new File(DUMP_PATH_1 + "/" + timeNow + ".png"));

                double similarity = calculateSimilarity(frame, mImagePath);
                if (maxSimilarity < similarity) {
                    maxSimilarity = similarity;
                    maxSimTime = timeNow;
                }

                grabber.setTimestamp(timeNow + (long)(1.0 * SEC));
                if (grabber.getTimestamp() >= startTime + INTERVAL) {  // 微秒
                    break;
                }
            }
            grabber.stop();

            System.out.println("Time: " + maxSimTime);
            System.out.println("Simi: " + maxSimilarity);
            return new Tuple2<>(maxSimTime, maxSimilarity);
        });

        // Get max similarity
        Tuple2<Long, Double> maxTuple = similarityRDD.reduce((x, y) -> {
            if (x._2 < y._2) {
                return y;
            } else {
                return x;
            }
        });

        // show max similarity
        System.out.println("MaxSimTime: " + maxTuple._1);
        System.out.println("MaxSimilarity: " + maxTuple._2);

    }

    /**
     *
     * @param frame the from some timestamp
     * @param imagePath the reference path
     * @return the bigger, the more similar (0 to 1)
     */
    private double calculateSimilarity(Frame frame, String imagePath) {
        // Convert Frame as source image
        final Mat src = new OpenCVFrameConverter.ToMat().convert(frame);

        // Get reference image
        final Mat ref = imread(imagePath);

        // Transfer to HSV
        cvtColor(src, src, CV_BGR2HSV);
        cvtColor(ref, ref, CV_BGR2HSV);

        // Arranging parameters for Histogram
        int h_bins = 50;
        int s_bins = 60;
        IntPointer histSize = new IntPointer(50, 60);

        FloatPointer h_ranges = new FloatPointer(0, 180);
        FloatPointer s_ranges = new FloatPointer(0, 256);
        PointerPointer ranges = new PointerPointer(h_ranges, s_ranges);

        IntPointer channels = new IntPointer(0, 1);
        Mat mask = new Mat();

        Mat hist_src = new Mat();
        Mat hist_ref = new Mat();

        // Get histograms and normalize them
        calcHist(src, 1, channels, mask, hist_src, 2, histSize, ranges, true, false);
        normalize(hist_src, hist_src, 0, 1, NORM_MINMAX, -1, new Mat());

        calcHist(ref, 1, channels, mask, hist_ref, 2, histSize, ranges, true, false);
        normalize(hist_ref, hist_ref, 0, 1, NORM_MINMAX, -1, new Mat());

        double similarity = compareHist(hist_src, hist_ref, CV_COMP_CORREL);

        return similarity;
    }

    /******************************************************************************************************************/
    public static void main(String[] args) throws FrameGrabber.Exception {
        new VideoSearcher().searchImageByVideo();
    }

}
