/**
 * Copyright 2011 Michael Cutler <m@cotdp.com>
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cotdp.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * This RecordReader implementation extracts individual files from a ZIP
 * file and hands them over to the Mapper. The "key" is the decompressed
 * file name, the "value" is the file contents.
 */
public class ZipFileRecordReader
        extends RecordReader<Text, Text> {
    /** InputStream used to read the ZIP file from the FileSystem */
    private FSDataInputStream fsin;

    /** ZIP file parser/decompresser */
    private ZipInputStream zip;

    /** Uncompressed file name */
    private Text currentKey;

    /** Uncompressed file contents */
    private Text currentValue;

    /** Used to indicate progress */
    private boolean isFinished = false;
    private ZipEntry currentEntry;
    private BufferedReader currentReader;
    private static final int CHUNK_SIZE = 2 * 512 * 1024;
    private enum ZipEntryStatus {
        NONE, CONSUMING, ENDED
    }
    private ZipEntryStatus currentStatus = ZipEntryStatus.NONE;

    /**
     * Initialise and open the ZIP file from the FileSystem
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {
        FileSplit split = (FileSplit) inputSplit;
        Configuration conf = taskAttemptContext.getConfiguration();
        Path path = split.getPath();
        FileSystem fs = path.getFileSystem(conf);

        // Open the stream
        fsin = fs.open(path);
        zip = new ZipInputStream(fsin);
    }

    /**
     * This is where the magic happens, each ZipEntry is decompressed and
     * readied for the Mapper. The contents of each file is held *in memory*
     * in a BytesWritable object.
     *
     * If the ZipFileInputFormat has been set to Lenient (not the default),
     * certain exceptions will be gracefully ignored to prevent a larger job
     * from failing.
     */
    @Override
    public boolean nextKeyValue()
            throws IOException, InterruptedException {
        // First step
        if (currentStatus == ZipEntryStatus.NONE) {
            stepNext();
        } else if (currentStatus == ZipEntryStatus.ENDED) {
            // Step to next zip entry
            stepNext();
        }
        // Sanity check
        if (currentEntry == null) {
            isFinished = true;
            return false;
        }
        currentKey = new Text(currentEntry.getName());
        currentStatus = ZipEntryStatus.CONSUMING;
        // Next Chunk
        StringBuilder builder = new StringBuilder();
        String nextLine = null;
        for (int i = 0; i < CHUNK_SIZE; i++) {
            nextLine = currentReader.readLine();
            if (nextLine == null) {
                currentStatus = ZipEntryStatus.ENDED;
                break;
            }
            builder.append(nextLine);
            builder.append("\n");
        }
        currentValue = new Text(builder.toString());
        return true;
    }

    private void stepNext() throws IOException {
        if (currentEntry != null) {
            zip.closeEntry();
        }
        currentEntry = zip.getNextEntry();
        currentReader = new BufferedReader(new InputStreamReader(zip));
    }

    /**
     * Rather than calculating progress, we just keep it simple
     */
    @Override
    public float getProgress()
            throws IOException, InterruptedException {
        return isFinished ? 1 : 0;
    }

    /**
     * Returns the current key (name of the zipped file)
     */
    @Override
    public Text getCurrentKey()
            throws IOException, InterruptedException {
        return currentKey;
    }

    /**
     * Returns the current value (contents of the zipped file)
     */
    @Override
    public Text getCurrentValue()
            throws IOException, InterruptedException {
        return currentValue;
    }

    /**
     * Close quietly, ignoring any exceptions
     */
    @Override
    public void close()
            throws IOException {
        try {
            zip.close();
        } catch (Exception ignore) {
        }
        try {
            fsin.close();
        } catch (Exception ignore) {
        }
    }
}
