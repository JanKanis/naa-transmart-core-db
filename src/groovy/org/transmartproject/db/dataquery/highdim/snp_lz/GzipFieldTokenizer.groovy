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
import com.google.common.base.Function;
import groovy.transform.CompileStatic;

import java.sql.Blob;
import java.util.zip.GZIPInputStream;

/**
 * Tokenizes a Blob field.
 */
@CompileStatic
class GzipFieldTokenizer {
    String version = 'Groovy version'

    private Blob blob
    private int expectedSize

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

    private <T> T withScanner(final Function<Scanner, T> action) {
        return withReader({ Reader r -> action.apply(new Scanner(r)) } as Function<Reader, T>)
    }

    public double[] asDoubleArray() {
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

    /**
     * @throws InputMismatchException iff the number of values read &ne; <var>expectedSize</var>.
     * @return a list of strings.
     */
    public List<String> asStringList() {
        return withReader({ Reader r ->
            ArrayList<String> res = new ArrayList<String>(expectedSize)
            StringBuilder builder = new StringBuilder();
            char c
            // The assignment expression takes the value of the right hand side
            while ((c = r.read()) >= 0) {
                if (c == ' ' as char) {
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
