package net.openhft.chronicle.bytes;

import net.openhft.chronicle.core.Maths;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;

public class Issue85Test {
    int different = 0;
    int different2 = 0;
    DecimalFormat df = new DecimalFormat();

    {
        df.setMaximumIntegerDigits(99);
        df.setMaximumFractionDigits(99);
        df.setGroupingUsed(false);
        df.setDecimalFormatSymbols(
                DecimalFormatSymbols.getInstance(Locale.ENGLISH));
    }

    static double parseDouble(Bytes bytes) {
        long value = 0;
        int deci = Integer.MIN_VALUE;
        while (bytes.readRemaining() > 0) {
            byte ch = bytes.readByte();
            if (ch == '.') {
                deci = 0;
            } else if (ch >= '0' && ch <= '9') {
                value *= 10;
                value += ch - '0';
                deci++;
            } else {
                break;
            }
        }
        if (deci <= 0) {
            return value;
        }
        return asDouble(value, deci);
    }

    private static double asDouble(long value, int deci) {
        int scale2 = 0;
        int leading = Long.numberOfLeadingZeros(value);
        if (leading > 1) {
            scale2 = leading - 1;
            value <<= scale2;
        }
        long fives = Maths.fives(deci);
        long whole = value / fives;
        long rem = value % fives;
        double d = whole + (double) rem / fives;
        double scalb = Math.scalb(d, -deci - scale2);
        return scalb;
    }

    @Test
    @Ignore("https://github.com/OpenHFT/Chronicle-Bytes/issues/85")
    public void bytesParseDouble_Issue85_Many0() {
        int max = 1000, count = 0;
        Bytes<ByteBuffer> bytes = Bytes.elasticHeapByteBuffer(64);
        for (double d0 = 1e9; d0 >= 1e-8; d0 /= 10) {
            long val = Double.doubleToRawLongBits(d0);
            for (int i = -max / 2; i < max / 2; i++) {
                double d = Double.longBitsToDouble(val + i);
                doTest(bytes, i, d);
            }
            count += max;
        }
        System.out.println("Different toString: " + 100.0 * different / count + "%," +
                " parsing: " + 100.0 * different2 / count + "%");
    }

    protected void doTest(Bytes<ByteBuffer> bytes, int i, double d) {
        String s = df.format(d);
        bytes.clear().append(s);
        double d2 = bytes.parseDouble();
        if (d != d2) {
            System.out.println(i + ": Parsing " + s + " != " + d2);
            ++different2;
        }
/*
        String s2 = bytes.append(d).toString();
        if (!s.equals(s2)) {
            System.out.println("ToString " + s + " != " + s2 + " should be " + new BigDecimal(d));
            ++different;
        }
*/
    }
}