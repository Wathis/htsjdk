package htsjdk.samtools.cram.encoding;

import htsjdk.HtsjdkTest;
import htsjdk.samtools.cram.io.*;
import htsjdk.samtools.cram.structure.EncodingID;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class ByteArrayLenCodecTest extends HtsjdkTest {

    // this is the only viable way to make a fully-functional ByteArrayLenEncoding
    // uses a modified version of ByteArrayLenEncoding toByteArray

    private ByteArrayLenEncoding makeEncoding(final int betaIntOffset, final int betaIntBitLimit, final byte valuesExternalBlockId) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream()) {

            byteArrayOutputStream.write((byte) EncodingID.BETA.ordinal());
            final byte[] lenBytes = new BetaIntegerEncoding(betaIntOffset, betaIntBitLimit).toByteArray();
            ITF8.writeUnsignedITF8(lenBytes.length, byteArrayOutputStream);
            byteArrayOutputStream.write(lenBytes);

            byteArrayOutputStream.write((byte) EncodingID.EXTERNAL.ordinal());
            final byte[] byteBytes = new byte[]{valuesExternalBlockId};
            ITF8.writeUnsignedITF8(byteBytes.length, byteArrayOutputStream);
            byteArrayOutputStream.write(byteBytes);

            ByteArrayLenEncoding bale = new ByteArrayLenEncoding();
            bale.fromByteArray(byteArrayOutputStream.toByteArray());
            return bale;
        }
    }

    // mock external data block compression header maps for the external byte array codecs

    private BitCodec<byte[]> makeWriteCodec(ByteArrayLenEncoding encoding, final int externalBlockId, ExposedByteArrayOutputStream mockExternalBlock) {
        final Map<Integer, ExposedByteArrayOutputStream> externalOutputMap = new HashMap<>();
        externalOutputMap.put(externalBlockId, mockExternalBlock);

        return encoding.buildCodec(null, externalOutputMap);
    }

    private BitCodec<byte[]> makeReadCodec(ByteArrayLenEncoding encoding, final int externalBlockId, final ByteArrayOutputStream mockExternalBlock) throws IOException {
        try (InputStream testIS = new ByteArrayInputStream(mockExternalBlock.toByteArray())) {

            final Map<Integer, InputStream> externalInputMap = new HashMap<>();
            externalInputMap.put(externalBlockId, testIS);

            return encoding.buildCodec(externalInputMap, null);
        }
    }

    @Test(dataProvider = "testByteArrays", dataProviderClass = IOTestCases.class)
    public void codecTest(byte[] values) throws IOException {

        final int betaIntOffset = 0;
        final int betaIntBitLimit = 32;
        final int externalBlockId = 0;

        ByteArrayLenEncoding encoding = makeEncoding(betaIntOffset, betaIntBitLimit, (byte)externalBlockId);

        try (ByteArrayOutputStream os = new ByteArrayOutputStream();
             BitOutputStream bos = new DefaultBitOutputStream(os);
             ExposedByteArrayOutputStream mockExternalBlock = new ExposedByteArrayOutputStream()) {

            // write values.length to bos using core encoding
            // write values to external block using external encoding
            BitCodec<byte[]> writeCodec = makeWriteCodec(encoding, externalBlockId, mockExternalBlock);
            writeCodec.write(bos, values);

            try (InputStream is = new ByteArrayInputStream(os.toByteArray());
                 DefaultBitInputStream dbis = new DefaultBitInputStream(is)) {

                // read values.length from dbis using core encoding
                // read values from external block using external encoding
                BitCodec<byte[]> readCodec = makeReadCodec(encoding, externalBlockId, mockExternalBlock);
                byte[] actual = readCodec.read(dbis);
                Assert.assertEquals(actual, values);
            }
        }
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void readWithLength() throws IOException {

        final int betaIntOffset = 0;
        final int betaIntBitLimit = 32;
        final int externalBlockId = 0;

        ByteArrayLenEncoding encoding = makeEncoding(betaIntOffset, betaIntBitLimit, (byte) externalBlockId);

        try (ExposedByteArrayOutputStream mockExternalBlock = new ExposedByteArrayOutputStream();
             InputStream is = new ByteArrayInputStream(new byte[0]);
             DefaultBitInputStream dbis = new DefaultBitInputStream(is)) {

            // read values.length from dbis using core encoding
            // read values from external block using external encoding
            BitCodec<byte[]> readCodec = makeReadCodec(encoding, externalBlockId, mockExternalBlock);
            readCodec.read(dbis, 1);
        }
    }
}