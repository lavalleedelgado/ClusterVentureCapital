package pld.ClusterVentureCapital;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import pld.IngestExemptOfferings.ExemptOffering;

public class SerializeExemptOfferings {

    /**
     * Serialize exempt offering filings on SEC form D for a Hive application. 
     */
    
    static TProtocol protocol;

    public static void main(String[] exemptOfferingsDirectory) {

        /**
         * Initialize a serializer and drive the ProcessExemptOfferings class.
         * 
         * exemptOfferingsDirectory (String): relative path to the directory.
         */

        final Configuration config = new Configuration();
        final TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());

        // Modify the class with a hash map of SequenceFile Writer objects.
        ProcessExemptOfferings processor = new ProcessExemptOfferings() {

            Map<Integer, SequenceFile.Writer> yearHashMap = new HashMap<Integer, SequenceFile.Writer>();

            Writer getYearWriter(Integer year) throws IOException {

                /**
                 * Identify and possibly initialize the SequenceFile Writer
                 * into which to serialize the data per corresponding year.
                 * 
                 * year (Integer): year correspondent to this filing.
                 */

                // Create the SequenceFile Writer for this year if it does not exist.
                if (!yearHashMap.containsKey(year)) {
                    yearHashMap.put(
                        year,
                        SequenceFile.createWriter(
                            config,
                            SequenceFile.Writer.file(new Path("/pld/data/form_d/form_d_" + year)),
                            SequenceFile.Writer.keyClass(IntWritable.class),
                            SequenceFile.Writer.valueClass(BytesWritable.class),
                            SequenceFile.Writer.compression(CompressionType.NONE)
                        )
                    );
                }

                // Return the SequenceFile Writer correspondent to the year.
                return yearHashMap.get(year);
            }

            // Modify the serializeFormD to identify the correct sequence file.
            @Override
            void serializeFormD(ExemptOffering exemptOfferingData, Integer year) throws IOException {
                try {
                    getYearWriter(year)
                    .append(
                        new IntWritable(1),
                        new BytesWritable(serializer.serialize(exemptOfferingData))
                    );
                } catch (TException e) {
                    throw new IOException(e);
                }
            }
        };

        // Serialize the data in the directory.
        try {
            processor.readDirectory(exemptOfferingsDirectory[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

