package myjavacv.spark;

import myjavacv.Global;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bytedeco.javacpp.opencv_core.*;
import org.bytedeco.javacv.*;
import scala.Tuple2;

import javax.imageio.ImageIO;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.bytedeco.javacpp.helper.opencv_imgcodecs.cvLoadImage;
import static org.bytedeco.javacpp.opencv_core.*;
import static org.bytedeco.javacpp.opencv_imgproc.*;

public class VideoSearcher implements Serializable {

    private final long SEC = 1000000;
    private final long INTERVAL = 30 * SEC;

    private final String DUMP_PATH_1 = "search_image_by_video";

    private String mImagePath = "winner.png";
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

                long timeNow = grabber.getTimestamp();
                ImageIO.write(new Java2DFrameConverter().convert(frame) , "png", new File(DUMP_PATH_1 + "/" + timeNow + ".png"));
//                double similarity = compareFrameToImage(frame, mImagePath);
                double similarity = compareFrameToImageOpenCV(frame, mImagePath);
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

            return new Tuple2<>(maxSimTime, maxSimilarity);
        });

        // After all map run and get similarities
        similarityRDD.foreach((t) -> {
            System.out.println("Time: " + t._1);
            System.out.println("Simi: " + t._2);
        });

//        double maxKey = similarityRDD.keys().max((x,y) -> {
//            if (x - y > 0) {
//                return 1;
//            } else if (x - y < 0) {
//                return -1;
//            } else {
//                return 0;
//            }
//        });
//
//        JavaPairRDD<Double,Long> withMaxKeys = similarityRDD.filter((x)->{
//            return x._1 == maxKey;
//        });
//
//        withMaxKeys.foreach((t)->{
//            System.out.print("MaxSim: " + t._1);
//            System.out.print("MaxTim: " + (t._2 / SEC));
//        });
    }

    private double compareFrameToImageOpenCV(Frame frame, String imagePath) {

        // Convert Frame to IplImage as source image
        OpenCVFrameConverter.ToIplImage converterToIplImage = new OpenCVFrameConverter.ToIplImage();
        final IplImage src = converterToIplImage.convert(frame);

        // Get reference image
        final IplImage ref = cvLoadImage(imagePath);

        //1
        IplImage hsvImageSrc = cvCreateImage(src.cvSize(), src.depth(), 3);
        cvCvtColor(src, hsvImageSrc, CV_BGR2HSV);
        // Split the 3 channels into 3 images
        IplImageArray hsvChannelsSrc = splitChannels(hsvImageSrc);
        //bins and value-range
        int numberOfBins = 50;
        float minRange = 0f;
        float maxRange = 180f;
        // Allocate histogram object
        int[]sizes = new int[]{numberOfBins};
        int histType = CV_HIST_ARRAY;
        float[] minMax = new  float[]{minRange, maxRange};
        float[][] ranges = new float[][]{minMax};
        CvHistogram histSrc = cvCreateHist(1, sizes, histType, ranges, 1);
        // Compute histogram
        cvCalcHist(hsvChannelsSrc.position(0), histSrc, 0, null);

        //2
        IplImage hsvImageRef= cvCreateImage(ref.cvSize(), ref.depth(), 3);
        cvCvtColor(ref, hsvImageRef, CV_BGR2HSV);
        // Split the 3 channels into 3 images
        IplImageArray hsvChannelsRef = splitChannels(hsvImageRef);
        // Allocate histogram object
        CvHistogram histRef = cvCreateHist(1, sizes, histType, ranges, 1);
        // Compute histogram
        cvCalcHist(hsvChannelsRef.position(0), histRef, 0, null);

        double similarity = compareHist(cvarrToMat(histRef.bins()), cvarrToMat(histSrc.bins()), CV_COMP_CORREL);

        return similarity;
    }

    /******************************************************************************************************************/

    /**
     *
     * @return the similarity between frame and image
     * @param frame the "Frame" object
     * @param imagePath the path of image to be compared
     */
    private double compareFrameToImage(Frame frame, String imagePath) {

        // Convert Frame to IplImage as source image
        OpenCVFrameConverter.ToIplImage converterToIplImage = new OpenCVFrameConverter.ToIplImage();
        final IplImage src = converterToIplImage.convert(frame);

        // Get histogram of src
        double[] hBinsSrc = new double[50];
        double[] sBinsSrc = new double[60];
        fillInHSBins(hBinsSrc, sBinsSrc, src);

        // Get reference image
        final IplImage ref = cvLoadImage(imagePath);

        // Get histogram of ref
        double[] hBinsRef = new double[50];
        double[] sBinsRef = new double[60];
        fillInHSBins(hBinsRef, sBinsRef, ref);

        double similarity = compareDis(hBinsSrc, sBinsSrc, hBinsRef, sBinsRef);

        return similarity;
    }

    /**
     * Get histogram in bins
     * @param hBins Hue Channel
     * @param sBins S Channel
     * @param img the image
     */
    private void fillInHSBins(double[] hBins, double[] sBins, IplImage img) {
        CvSize size = img.cvSize();
        int depth = img.depth();
        int hBinNum = hBins.length;
        int sBinNum = sBins.length;

        // Create empty image (placeholder)
        final IplImage hsvImage = cvCreateImage(size, depth, 3);
        // Convert "img" (BGR) to "HSV" form
        cvCvtColor(img, hsvImage, CV_BGR2HSV);

        // Start get histogram
        for (int i = 0; i < size.height(); i++) {
            for (int j = 0; j < size.width(); j++) {
                CvScalar s = cvGet2D(hsvImage, i, j);
                double hVal = s.val(0);
                double sVal = s.val(1);
                hBins[(int)(hVal / 180 * hBinNum)] += 1.0;
                sBins[(int)(sVal / 256 * sBinNum)] += 1.0;
            }
        }

        // Normalization
        int all = size.height() * size.width();
        for (int i = 0; i < hBinNum; i++) {
            hBins[i] /= all;
        }
        for (int i = 0; i < sBinNum; i++) {
            sBins[i] /= all;
        }
    }

    /**
     * Compare Former half bins to Later half bins (Manhattan Distance)
     * @param bins
     * @return
     */
    private double compareDis(double[]... bins) {
        int n = bins.length;
        double distance = 0;
        double maxDistance = n;
        for (int i = 0, j = n/2; j < n; i++, j++) {
            int len = bins[i].length;
            for (int k = 0; k < len ;k++) {
                distance += Math.abs(bins[i][k] - bins[j][k]);
            }
        }
        double similarity = 1 - distance / maxDistance;
        return similarity;
    }

    private IplImageArray splitChannels(IplImage hsvImage) {
        CvSize size = hsvImage.cvSize();
        int depth=hsvImage.depth();
        IplImage channel0 = cvCreateImage(size, depth, 1);
        IplImage channel1 = cvCreateImage(size, depth, 1);
        IplImage channel2 = cvCreateImage(size, depth, 1);
        cvSplit(hsvImage, channel0, channel1, channel2, null);
        return new IplImageArray(channel0, channel1, channel2);
    }

    public static void main(String[] args) throws FrameGrabber.Exception {
        new VideoSearcher().searchImageByVideo();
    }

}
