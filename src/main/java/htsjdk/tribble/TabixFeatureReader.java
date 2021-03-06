/*
 * The MIT License
 *
 * Copyright (c) 2013 The Broad Institute
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package htsjdk.tribble;

import htsjdk.samtools.seekablestream.SeekableStreamFactory;
import htsjdk.samtools.util.BlockCompressedInputStream;
import htsjdk.samtools.util.RuntimeIOException;
import htsjdk.tribble.readers.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * @author Jim Robinson
 * @since 2/11/12
 */
public class TabixFeatureReader<T extends Feature, SOURCE> extends AbstractFeatureReader<T, SOURCE> {

    TabixReader tabixReader;
    List<String> sequenceNames;

    /**
     * @param featureFile - path to a feature file. Can be a local file, http url, or ftp url
     * @param codec
     * @throws IOException
     */
    public TabixFeatureReader(final String featureFile, final AsciiFeatureCodec codec) throws IOException {
        this(featureFile, null, codec, null, null);
    }

    /**
     * @param featureFile - path to a feature file. Can be a local file, http url, or ftp url
     * @param indexFile   - path to the index file.
     * @param codec
     * @throws IOException
     */
    public TabixFeatureReader(final String featureFile, final String indexFile, final AsciiFeatureCodec codec) throws IOException {
        this(featureFile, indexFile, codec, null, null);
    }

    /**
     * @param featureFile  path to a feature file. Can be a local file, http url, or ftp url
     * @param indexFile    path to the index file.
     * @param wrapper      a wrapper to apply to the byte stream from the featureResource allowing injecting features
     *                     like caching and prefetching of the stream, may be null, will only be applied if featureFile
     *                     is a uri representing a {@link java.nio.file.Path}
     * @param indexWrapper a wrapper to apply to the byte stream from the indexResource, may be null, will only be
     *                     applied if indexFile is a uri representing a {@link java.nio.file.Path}
     */
    public TabixFeatureReader(final String featureFile, final String indexFile, final AsciiFeatureCodec codec,
                              final Function<SeekableByteChannel, SeekableByteChannel> wrapper,
                              final Function<SeekableByteChannel, SeekableByteChannel> indexWrapper) throws IOException {
        super(featureFile, codec, wrapper, indexWrapper);
        tabixReader = new TabixReader(featureFile, indexFile, wrapper, indexWrapper);
        sequenceNames = new ArrayList<>(tabixReader.getChromosomes());
        readHeader();
    }

    /**
     * read the header
     *
     * @return a Object, representing the file header, if available
     * @throws IOException throws an IOException if we can't open the file
     */
    private void readHeader() throws IOException {
        SOURCE source = null;
        try {
            source = codec.makeSourceFromStream(new PositionalBufferedStream(new BlockCompressedInputStream(SeekableStreamFactory.getInstance().getStreamFor(path, wrapper))));
            header = codec.readHeader(source);
        } catch (Exception e) {
            throw new TribbleException.MalformedFeatureFile("Unable to parse header with error: " + e.getMessage(), path, e);
        } finally {
            if (source != null) {
                codec.close(source);
            }
        }
    }

    @Override
    public boolean hasIndex() {
        return true;
    }

    @Override
    public List<String> getSequenceNames() {
        return sequenceNames;
    }

    /**
     * Return iterator over all features overlapping the given interval
     *
     * @param chr
     * @param start
     * @param end
     * @return
     * @throws IOException
     */
    @Override
    public CloseableTribbleIterator<T> query(final String chr, final int start, final int end) throws IOException {
        final List<String> mp = getSequenceNames();
        if (mp == null) throw new TribbleException.TabixReaderFailure("Unable to find sequence named " + chr +
                " in the tabix index. ", path);
        if (!mp.contains(chr)) {
            return new EmptyIterator<T>();
        }
        final TabixIteratorLineReader lineReader = new TabixIteratorLineReader(tabixReader.query(tabixReader.chr2tid(chr), start - 1, end));
        return new FeatureIterator<T>(lineReader, start - 1, end);
    }

    @Override
    public CloseableTribbleIterator<T> iterator() throws IOException {
        final InputStream is = new BlockCompressedInputStream(SeekableStreamFactory.getInstance().getStreamFor(path, wrapper));
        final PositionalBufferedStream stream = new PositionalBufferedStream(is);
        final LineReader reader = new SynchronousLineReader(stream);
        return new FeatureIterator<T>(reader, 0, Integer.MAX_VALUE);
    }

    @Override
    public void close() throws IOException {
        tabixReader.close();
    }

    class FeatureIterator<T extends Feature> implements CloseableTribbleIterator<T> {
        private T currentRecord;
        private LineReader lineReader;
        private int start;
        private int end;

        public FeatureIterator(final LineReader lineReader, final int start, final int end) throws IOException {
            this.lineReader = lineReader;
            this.start = start;
            this.end = end;
            readNextRecord();
        }

        /**
         * Advance to the next record in the query interval.
         *
         * @throws IOException
         */
        protected void readNextRecord() throws IOException {
            currentRecord = null;
            String nextLine;
            while (currentRecord == null && (nextLine = lineReader.readLine()) != null) {
                final Feature f;
                try {
                    f = ((AsciiFeatureCodec) codec).decode(nextLine);
                    if (f == null) {
                        continue;   // Skip
                    }
                    if (f.getStart() > end) {
                        return;    // Done
                    }
                    if (f.getEnd() <= start) {
                        continue;   // Skip
                    }

                    currentRecord = (T) f;

                } catch (TribbleException e) {
                    e.setSource(path);
                    throw e;
                } catch (NumberFormatException e) {
                    String error = "Error parsing line: " + nextLine;
                    throw new TribbleException.MalformedFeatureFile(error, path, e);
                }
            }
        }

        @Override
        public boolean hasNext() {
            return currentRecord != null;
        }

        @Override
        public T next() {
            T ret = currentRecord;
            try {
                readNextRecord();
            } catch (IOException e) {
                throw new RuntimeIOException("Unable to read the next record, the last record was at " +
                        ret.getContig() + ":" + ret.getStart() + "-" + ret.getEnd(), e);
            }
            return ret;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Remove is not supported in Iterators");
        }

        @Override
        public void close() {
            lineReader.close();
        }

        @Override
        public Iterator<T> iterator() {
            return this;
        }
    }
}
