package org.neu.pdpmr.tasks.ml;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;

import org.neu.pdpmr.tasks.types.Flight;
import weka.classifiers.trees.RandomForest;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.CSVLoader;

/**
 * Machine Learning model used 
 * @author yuwen
 *
 */
public class MLmodel {

    private RandomForest model;
    public Instances trainSet;

    public static void main(String[] args) throws Exception {

        String hdfsData = "out/results/hdfsTrainData.csv";
        String targetSet = "out/results/test-90.csv";
        MLmodel.convertHdfsDatToCsv(hdfsData, targetSet);

        String trainSetPath = "out/results/test-small.csv";
        MLmodel ml = new MLmodel(trainSetPath);

        System.out.println("validating precision...");
        float trainPre = ml.getPrecision(ml.trainSet);
        System.out.println("Training precision : " + trainPre);

    }

    public MLmodel(String trainSetPath) throws Exception {
        CSVLoader loader = new CSVLoader();
        loader.setSource(new File(trainSetPath));

        this.trainSet = loader.getDataSet();
        // label is in the first colum
        trainSet.setClassIndex(0);
        int num = trainSet.numAttributes();
        this.train();
    }

    /**
     * interface for using the model
     * @param f   flight that are going to be predicted
     * @return predictDelay predicted Delay in  minutes
     * @throws Exception  when new nominal data are received
     */
    public double predictDelay(Flight f) throws Exception {

        try {
            int delay = (int)f.delayMs;
            String year = new SimpleDateFormat("YYYY").format(f.depTimeMs);
            String month = new SimpleDateFormat("mm").format(f.depTimeMs);
            String day = new SimpleDateFormat("dd").format(f.depTimeMs);
            int crsDepTime = (int)f.depTimeMs;
            int crsArrTime = (int)f.arrTimeMs;
            String carrier = f.carrier.toString();
            String orig = f.orig.toString();
            String dest = f.dest.toString();

            Instance instance = trainSet.firstInstance();
            instance.setValue(trainSet.attribute("delay"), delay);
            instance.setValue(trainSet.attribute("year"), year);
            instance.setValue(trainSet.attribute("month"), month);
            instance.setValue(trainSet.attribute("day"), day);
            instance.setValue(trainSet.attribute("crsDepTime"), crsDepTime);
            instance.setValue(trainSet.attribute("crsArrTime"), crsArrTime);
            instance.setValue(trainSet.attribute("carrier"), carrier);
            instance.setValue(trainSet.attribute("orig"), orig);
            instance.setValue(trainSet.attribute("dest"), dest);

            return model.classifyInstance(instance);
        } catch (Exception e) {
            // weka model is not capable of dealing with new nominal data such as carrier,
            // orig, dest. In this case we return a high number to avoid making an
            // optimistic error.
            e.printStackTrace();
            return 50;
        }
    }

    private void train() throws Exception {

        model = new RandomForest();
        model.setMaxDepth(10);
        System.out.println("Training a random forest ...");
        long stime = System.currentTimeMillis();
        model.buildClassifier(this.trainSet);
        System.out.println(
                "Training completed. Time used : " + (System.currentTimeMillis() - stime) / 1000 + " seconds.");
    }

    /**
     * convert the local csv file copied from hdfs to an available training file
     *
     * @param sourcePath path of the local csv file copied from hdfs
     * @param targetPath path of the available training file
     * @throws IOException
     */
    static void convertHdfsDatToCsv(String sourcePath, String targetPath) throws IOException {

        BufferedWriter writer = new BufferedWriter(new FileWriter(targetPath));
        // output header
        writer.write("delay,year,month,day,crsDepTime,crsArrTime,carrier,orig,dest");
        // output hdfs data as data
        BufferedReader reader = new BufferedReader(new FileReader(sourcePath));
        String line = null;
        while ((line = reader.readLine()) != null) {
            writer.newLine();
            writer.write(line);
        }
        writer.close();
        reader.close();
    }

    // like the convertHdfsDatToCsv, convert the data into arff format
    static void convertHdfsDatToArff(String sourcePath, String targetPath) throws IOException {

        BufferedWriter writer = new BufferedWriter(new FileWriter(targetPath));
        // output header
        writer.write("@RELATION flightDelay");
        writer.newLine();
        writer.write("@ATTRIBUTE delay NUMERIC");
        writer.newLine();
        writer.write("@ATTRIBUTE year NUMERIC");
        writer.newLine();
        writer.write("@ATTRIBUTE month NUMERIC");
        writer.newLine();
        writer.write("@ATTRIBUTE day NUMERIC");
        writer.newLine();
        writer.write("@ATTRIBUTE crsDepTime NUMERIC");
        writer.newLine();
        writer.write("@ATTRIBUTE crsArrTime NUMERIC");
        writer.newLine();
        writer.write("@ATTRIBUTE carrier string");
        writer.newLine();
        writer.write("@ATTRIBUTE orig string");
        writer.newLine();
        writer.write("@ATTRIBUTE dest string");
        writer.newLine();
        writer.write("@DATA");
        // output hdfs data as data
        BufferedReader reader = new BufferedReader(new FileReader(sourcePath));
        String line = null;
        while ((line = reader.readLine()) != null) {
            writer.newLine();
            writer.write(line);
        }
        writer.close();
        reader.close();
    }

    // return the precision
    private float getPrecision(Instances dataSet) throws Exception {

        float size = (float) dataSet.size();
        float correct = 0;
        for (Instance ins : dataSet) {
            int res = (int) model.classifyInstance(ins);
            if ((int) ins.classValue() > 0) {
                if (res > 0)
                    correct += 1;
            } else {
                if (res == 0)
                    correct += 1;
            }
        }
        return correct / size;
    }
}
