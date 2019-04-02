package myjavacv.spark;

import myjavacv.Global;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class SingleVideoClipLauncher {

    public static final String FILE_NAME = "EP00.mp4";
    public static final long SEC = 1000000;
    public static final long INTERVAL = 30 * SEC;

    public static List<Long> startTimeStamps;

    public static void main(String[] args) throws FrameGrabber.Exception {

        // History Dump File delete
        File dumpDir = new File("spark-clip-dump");
        if (dumpDir.exists()) {
            Global.deleteDir(dumpDir);
        }
        dumpDir.mkdirs();

        FFmpegFrameGrabber grabber4Init = new FFmpegFrameGrabber(FILE_NAME);
        grabber4Init.start();
        long length = grabber4Init.getLengthInTime();
        grabber4Init.stop();

        // Arrange start timestamps
        startTimeStamps = new ArrayList<>();
        for (long i = 0;  i < length; i += INTERVAL) {
            startTimeStamps.add(i);
        }

        // Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Cut Clip");

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Long> startTimeStampRDD = sc.parallelize(startTimeStamps);
        startTimeStampRDD.foreach((startTime)->{
            System.out.println("Task [" + startTime + " to " + Math.min(startTime + INTERVAL, length) + ")");

            FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(FILE_NAME);
            grabber.start();
            if (startTime != 0) {
                grabber.setTimestamp(startTime);
            }

            FFmpegFrameRecorder recorder = new FFmpegFrameRecorder("spark-clip-dump/" + startTime + "-" + FILE_NAME, grabber.getImageWidth(), grabber.getImageHeight(), 0);
            recorder.setVideoBitrate(grabber.getVideoBitrate());
            recorder.setVideoCodec(grabber.getVideoCodec());
            recorder.setFormat(grabber.getFormat());
            recorder.setFrameRate(grabber.getFrameRate());

//            recorder.setAudioBitrate(grabber.getAudioBitrate());
//            recorder.setSampleRate(grabber.getSampleRate());
//            recorder.setAudioCodec(grabber.getAudioCodec());
            recorder.start();

            while (true) {
                Frame frame = grabber.grabImage();
                if (frame == null) {
                    break;
                }

                recorder.record(frame);

                if (grabber.getTimestamp() >= startTime + INTERVAL) {  // 微秒
                    break;
                }
            }

            recorder.stop();
            grabber.stop();
        });

    }

}
