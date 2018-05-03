package main.java;

import java.io.*;
public class ReadWrite {

      static String readFileName;
      static String writeFileName;


      public static void main(String args[]){
           readFileName = args[0];
           writeFileName = args[1];
          try {
             // readInput();
            readFileByLines(readFileName);
          }catch(Exception e){
          }
      }

    public static void readFileByLines(String fileName) {
        FileInputStream fis = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        String tempString = null;
        try {
            System.out.println("以行为单位读取文件内容，一次读一整行：");
            fis = new FileInputStream(fileName);// FileInputStream
            // 从文件系统中的某个文件中获取字节
            isr = new InputStreamReader(fis,"GBK");
            br = new BufferedReader(isr);
            int count=0;
            while ((tempString = br.readLine()) != null) {
                count++;
                // 显示行号
                Thread.sleep(300);
                String str = new String(tempString.getBytes("UTF8"),"GBK");
                System.out.println("row:"+count+">>>>>>>>"+tempString);
                method1(writeFileName,tempString);
                //appendMethodA(writeFileName,tempString);
            }
            isr.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            if (isr != null) {
                try {
                    isr.close();
                } catch (IOException e1) {
                }
            }
        }
    }

//    public static void appendMethodA(String fileName, String content) {
//        try {
//            // 打开一个随机访问文件流，按读写方式
//            //logger.info("file line >>>>>>>>>>>>>>>>>>>>>>>>>:"+content);
//            RandomAccessFile randomFile = new RandomAccessFile(fileName, "rw");
//
//            // 文件长度，字节数
//            long fileLength = randomFile.length();
//            //将写文件指针移到文件尾。
//            randomFile.seek(fileLength);
//            //randomFile.writeUTF(content);
//            randomFile.writeUTF(content);
//            randomFile.writeUTF("\n");
//           // randomFile.wri;
//
//            randomFile.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }

    public static void method1(String file, String conent) {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file, true)));
            out.write("\n");
            out.write(conent);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }




}
