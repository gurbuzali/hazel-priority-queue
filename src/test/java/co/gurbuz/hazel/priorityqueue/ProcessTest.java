package co.gurbuz.hazel.priorityqueue;

import java.io.File;
import java.io.InputStream;

/**
 * @ali 10/11/13
 */
public class ProcessTest {

    public static void main(String[] args) throws Exception {
        int counter = 1;

        int exit = 1;
        while (exit > 0) {
            System.err.println("\n\n\t\t ---- start of process asdf counter: " + counter + " \n\n");
            exit = startProcess("gurbuz.GurbuzTest");
            System.err.println("\n\n\t\t ---- end of process asdf counter: " + counter + "\n\n");
            counter++;
        }

    }

    public static int startProcess(String className) throws Exception {
        final String property = System.getProperty("java.class.path");

        String[] command = {"java","-cp", "\"" + property + "\"", className};
        ProcessBuilder proBuilder = new ProcessBuilder( command ).redirectErrorStream(true);
        proBuilder.directory(new File("/java/workspace/test/target/classes"));


        Process process = proBuilder.start();

        InputStream in = process.getInputStream();

        byte[] data = new byte[1024];

        int len = -1;

        while ((len = in.read(data)) != -1 ){
            System.err.print(new String(data, 0 , len));
        }

        return process.waitFor();
    }

}
