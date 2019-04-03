package myjavacv;

import java.io.File;

public class Global {

    public static void deleteAndMkdirs(File dir) {
        if (dir.exists()) {
            deleteDir(dir);
        }
        dir.mkdirs();
    }

    public static void deleteDir(File dir) {
        if (!dir.exists()) {
            return;
        }
        if (dir.isDirectory()) {
            for (File file: dir.listFiles()) {
                deleteDir(file);
            }
            dir.delete();
        } else {
            dir.delete();
        }
    }

}
