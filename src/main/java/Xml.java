/**
 * Created by Nicolas on 11/04/2017.
 */
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;


public class Xml {
    public static void main(String[] args){
        SparkContext sc = new SparkContext();

        SQLContext sqlc = new SQLContext(sc);
        DataFrameReader a = sqlc.read()
                .format("com.databricks.spark.xml")
                .option("path", "hdfs://192.168.56.10:8020/user/hdfs/Etam/orders2017/orders2017.xml")
                .option("rowTag","order");

        Dataset<Row> v = a.load();

        v.show();

        System.out.println("Nombre de lignes : "+v.count());

    }
}
