package org.demo;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.*;
import org.apache.hadoop.ozone.client.io.OzoneInputStream;
import org.apache.hadoop.ozone.client.io.OzoneOutputStream;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.File;

public class AccessOzoneFS {
    public static void main(String[] args) {

        try {
            OzoneConfiguration ozoneConfiguration = new OzoneConfiguration();
            String omLeaderAddress =  args[0];
            String omPrincipal =  args[1];
            String keytabPathLocal =  args[2];
            String volume =  args[3];
            String bucket =  args[4];
            String key =  args[5];
            String sourceFilePath =  args[6];
            Long dataSize =  Long.parseLong(args[7]);



            //set om leader node
            ozoneConfiguration.set("ozone.om.address", omLeaderAddress);

            // Setting kerberos authentication
            ozoneConfiguration.set("ozone.om.kerberos.principal.pattern", "*");
            ozoneConfiguration.set("ozone.security.enabled", "true");
            ozoneConfiguration.set("hadoop.rpc.protection", "privacy");
            ozoneConfiguration.set("hadoop.security.authentication", "kerberos");
            ozoneConfiguration.set("hadoop.security.authorization", "true");

            //Passing keytab for Authentication
            UserGroupInformation.setConfiguration(ozoneConfiguration);
            UserGroupInformation.loginUserFromKeytab(omPrincipal, keytabPathLocal);

            OzoneClient ozClient = OzoneClientFactory.getRpcClient(ozoneConfiguration);
            ObjectStore objectStore = ozClient.getObjectStore();


            // Let us create a volume to store buckets.
            objectStore.createVolume(volume);

            // Let us verify that the volume got created.
            OzoneVolume assets = objectStore.getVolume(volume);
            // Let us create a bucket called bucket.
            assets.createBucket(bucket);
            OzoneBucket video = assets.getBucket(bucket);
            // read data from the file, this is assumed to be a user provided function.
            byte[] videoData = FileUtils.readFileToByteArray(new File(sourceFilePath));

            // Create an output stream and write data.
            OzoneOutputStream videoStream = video.createKey(key, dataSize.longValue());
            videoStream.write(videoData);

            // Close the stream when it is done.
            videoStream.close();
            // We can use the same bucket to read the file that we just wrote, by creating an input Stream.
            // Let us allocate a byte array to hold the video first.
            byte[] data = new byte[(int) dataSize.longValue()];
            OzoneInputStream introStream = video.readKey(key);
            introStream.read(data);

            // Close the stream when it is done.
            introStream.close();

            // Close the client.
            ozClient.close();

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}