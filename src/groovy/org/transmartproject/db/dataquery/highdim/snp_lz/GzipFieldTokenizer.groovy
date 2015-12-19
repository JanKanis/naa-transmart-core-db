/*
 * Copyright © 2013-2015 The Hyve B.V.
 *
 * This file is part of transmart-core-db.
 *
 * Transmart-core-db is free software: you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the Free
 * Software Foundation, either version 3 of the License, or (at your option) any
 * later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * transmart-core-db.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.transmartproject.db.dataquery.highdim.snp_lz;

import com.google.common.base.Charsets;
import com.google.common.base.Function
import com.google.common.collect.AbstractIterator
import groovy.transform.CompileStatic;

import java.sql.Blob;
import java.util.zip.GZIPInputStream;

/**
 * Tokenizes a Blob field.
 */
@CompileStatic
class GzipFieldTokenizer {
    String version = 'Groovy version, withTokens function'

    private Blob blob
    private int expectedSize

    static final char space = ' ' as char

    public GzipFieldTokenizer(Blob blob, int expectedSize) {
        this.blob = blob
        this.expectedSize = expectedSize
    }

    private <T> T withReader(Function<Reader, T> action) {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(blob.getBinaryStream()), Charsets.US_ASCII));

        try {
            return action.apply(reader)
        } finally {
            reader.close()
        }
    }

    private void withTokens(Function<String, Object> closure) {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(blob.getBinaryStream()), Charsets.US_ASCII));

        try {
            StringBuilder builder = new StringBuilder();
            int size = 0
            char c
            // The assignment expression takes the value of the right hand side
            while ((c = reader.read()) >= 0) {
                if (c == space) {
                    size++
                    if (size > expectedSize - 1) {
                        throw new InputMismatchException("Got more tokens than the $expectedSize expected")
                    }
                    closure.apply(builder.toString())
                    builder.setLength(0)
                } else {
                    builder.append(c)
                }
            }

            size += (size > 0 || builder.size() ? 1 : 0)
            // check first to make sure we don't call closure too many times
            if (size != expectedSize) {
                throw new InputMismatchException("Expected $expectedSize tokens, but got only $size")
            }
            if (size) {
                closure.apply(builder.toString())
            }
        } finally {
            reader.close()
        }
    }

    private <T> T withIterator(Function<Iterator<String>, T> action) {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(new GZIPInputStream(blob.getBinaryStream()), Charsets.US_ASCII));

        try {
            return action.apply((Iterator<String>) new AbstractIterator<String>() {

                private StringBuilder builder = new StringBuilder();
                private int size = 0;

                private String value(boolean last) {
                    size++
                    if (size > expectedSize - (last ? 0 : 1)) {
                        throw new InputMismatchException("Got more tokens than the $expectedSize expected")
                    }
                    if (last) {
                        if (size != expectedSize) {
                            throw new InputMismatchException("Expected $expectedSize tokens, but got only $size}")
                        }
                    }
                    String ret = builder.toString()
                    builder.setLength(0)
                    // I am really missing an AbstractIterator.lastValue(T) here, so manage state manually
                    if (last) builder = null
                    return ret
                }

                @Override
                String computeNext() {
                    char c
                    // The assignment expression takes the value of the right hand side
                    while ((c = reader.read()) >= 0) {
                        if (c == space) {
                            return value(false)
                        } else {
                            builder.append(c)
                        }
                    }
                    // If the reader was entirely empty, don't yield anything
                    if (builder && (size || builder.size())) {
                        return value(true)
                    }
                    return endOfData()
                }
            })
        } finally {
            reader.close()
        }
    }

    private <T> T withScanner(final Function<Scanner, T> action) {
        return withReader({ Reader r -> action.apply(new Scanner(r)) } as Function<Reader, T>)
    }

    public double[] asDoubleArray() {
        double[] res = new double[expectedSize]
        int i = 0
        withTokens(new Function<String,Object>(){Object apply(String tok) {
            setDoubleAt(res, i++, Double.parseDouble(tok))
        }})
        return res
    }

    // Doing this inline in a closure confuses the groovy compiler
    static private void setDoubleAt(double[] arr, int index, double d) {
        arr[index] = d
    }

    public double[] asDoubleArray2() {
        return withIterator(new Function<Iterator<String>, double[]>() { double[] apply(Iterator<String> tokens) {
            double[] res = new double[expectedSize]
            int i = 0
            while (tokens.hasNext()) {
                res[i++] = Double.parseDouble(tokens.next())
            }
            return res;
        }})
    }

    public double[] asDoubleArray1() {
        return withScanner({ Scanner scan ->
            double[] res = new double[expectedSize]
            int i = 0
            while (scan.hasNext()) {
                if (i > expectedSize - 1) {
                    throw new InputMismatchException("Got more tokens than the $expectedSize expected")
                }
                // do not use parseDouble, otherwise the scanner will just
                // refuse to consume input that doesn't look like a float
                String nextToken = scan.next()
                res[i++] = Double.parseDouble(nextToken)
            }
            if (i < expectedSize) {
                throw new InputMismatchException("Expected $expectedSize tokens, but got only ${i-1}")
            }

            return res;
        } as Function<Scanner, double[]>)
    }

    public List<String> asStringList() {
        ArrayList<String> res = new ArrayList(expectedSize)
        withTokens(new Function<String,Object>(){ Object apply(String tok) {
            res.add(tok)
        }})
        return res
    }

    /**
     * @throws InputMismatchException iff the number of values read &ne; <var>expectedSize</var>.
     * @return a list of strings.
     */
    public List<String> asStringList2() {
        withIterator(new Function<Iterator<String>, List<String>>() { List<String> apply(Iterator<String> iter) {
        //Iterator<String> iter ->
            ArrayList<String> l = new ArrayList(expectedSize)
            while (iter.hasNext()) {
                l.add(iter.next())
            }
            return l
        }})
        //})
    }

    public List<String> asStringList1() {
        return withReader({ Reader r ->
            ArrayList<String> res = new ArrayList<String>(expectedSize)
            StringBuilder builder = new StringBuilder();
            char c
            // The assignment expression takes the value of the right hand side
            while ((c = r.read()) >= 0) {
                if (c == space) {
                    res.add(builder.toString())
                    builder.setLength(0)
                    if (res.size() > expectedSize - 1) {
                        throw new InputMismatchException("Got more tokens than the $expectedSize expected")
                    }
                } else {
                    builder.append(c)
                }
            }

            if (res.size() > 0 || builder.size() > 0) {
                res.add(builder.toString())
            }
            if (res.size() != expectedSize) {
                throw new InputMismatchException("Expected $expectedSize tokens, but got only ${res.size()}")
            }

            return res
        } as Function<Reader, List<String>>)
    }

}
