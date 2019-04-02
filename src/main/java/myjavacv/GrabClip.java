package myjavacv;

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.FFmpegFrameRecorder;
import org.bytedeco.javacv.Frame;

import java.io.File;
import java.io.IOException;

public class GrabClip {

    public static void main(String[] args) throws IOException {

        File dumpDir = new File("clip-dump");
        if (dumpDir.exists()) {
            Global.deleteDir(dumpDir);
        }
        dumpDir.mkdirs();

        FFmpegFrameGrabber grabber = new FFmpegFrameGrabber("EP00.mp4");

        grabber.start();
        grabber.setTimestamp(25000000); // 微秒

        FFmpegFrameRecorder recorder = new FFmpegFrameRecorder("clip-dump/EP00.output.mp4", grabber.getImageWidth(), grabber.getImageHeight(), grabber.getAudioChannels());
        recorder.setVideoBitrate(grabber.getVideoBitrate());
        recorder.setVideoCodec(grabber.getVideoCodec());
        recorder.setFormat(grabber.getFormat());
        recorder.setFrameRate(grabber.getFrameRate());

        recorder.setAudioBitrate(grabber.getAudioBitrate());
        recorder.setSampleRate(grabber.getSampleRate());
        recorder.setAudioCodec(grabber.getAudioCodec());
        recorder.start();

        while (true) {
            Frame frame = grabber.grab();
            if (frame == null) {
                break;
            }

            recorder.record(frame);

            if (grabber.getTimestamp() >= 35000000) {  // 微秒
                break;
            }
        }

        recorder.stop();
        grabber.stop();
    }

}
