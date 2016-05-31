/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.lerningstorm;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 *
 * @author vahan
 */
public class LearningStormBolt extends BaseBasicBolt {

    private static final long serialVersionUID = 1L;

    
    private static void appendUsingFileWriter(String filePath, String text) {
        File file = new File(filePath);
        FileWriter fr = null;
        try {
            fr = new FileWriter(file,true);
            fr.write(text+"\n");
           
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            try {
                fr.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }    
    
    public void execute(Tuple input, BasicOutputCollector collector) {
// fetched the field "site" from input tuple.
//        String test = input.getStringByField("site");
        String test = input.getString(0);
// print the value of field "site" on console.
        System.out.println("Name of input site is : " + test);
        String fileName = "/tmp/LearningStormBolt.txt";
        //Определяем файл
        File file = new File(fileName);

        try {
            //проверяем, что если файл не существует то создаем его
            if (!file.exists()) {
                file.createNewFile();
            }
            
            appendUsingFileWriter(fileName, "Name of input site is : " + test);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
