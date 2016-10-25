/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.tika;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URL;


public class TestConvertDocumentToText {

    private TestRunner runner;

    @Before
    public void init() {
        runner = TestRunners.newTestRunner(ConvertDocumentToText.class);
    }

    @Test
    public void testProcessor() {

    }

    @Test
    public void testDocExtraction() throws FileNotFoundException {
        String expected = "test\n\n";
        InputStream is = getFileStream("test.doc");
        runner.enqueue(is);
        runner.run();
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0)
                .assertContentEquals(expected, "UTF-8");
    }

    @Test
    public void testDocxExtraction() throws FileNotFoundException {
        String expected = "test\n";
        InputStream is = getFileStream("test.docx");
        runner.enqueue(is);
        runner.run();
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0)
                .assertContentEquals(expected, "UTF-8");
    }

    @Test
    public void testRtfExtraction() throws FileNotFoundException {
        String expected = "test\n";
        InputStream is = getFileStream("test.rtf");
        runner.enqueue(is);
        runner.run();
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0).assertContentEquals(expected, "UTF-8");
    }

    @Test
    public void testTextExtraction() throws FileNotFoundException {
        String expected = "test\n\n";
        InputStream is = getFileStream("test.txt");
        runner.enqueue(is);
        runner.run();
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0).assertContentEquals(expected, "UTF-8");
    }

    @Test
    public void testBasicPdfExtraction() throws FileNotFoundException {
        String expected = "\ntest\n\n\n";
        InputStream is = getFileStream("test-basic.pdf");
        runner.enqueue(is);
        runner.run();
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0).assertContentEquals(expected, "UTF-8");
    }

    @Test
    public void testArchivePdfExtraction() throws FileNotFoundException {
        String expected = "\ntest\n\n\n";
        InputStream is = getFileStream("test-archive.pdf");
        runner.enqueue(is);
        runner.run();
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0).assertContentEquals(expected, "UTF-8");
    }

    @Test
    public void testHybridPdfExtraction() throws FileNotFoundException {
        String expected = "\ntest\n\n\n";
        InputStream is = getFileStream("test-hybrid.pdf");
        runner.enqueue(is);
        runner.run();
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0).assertContentEquals(expected, "UTF-8");
    }

    @Test
    public void testTaggedPdfExtraction() throws FileNotFoundException {
        String expected = "\ntest\n\n\n";
        InputStream is = getFileStream("test-tagged.pdf");
        runner.enqueue(is);
        runner.run();
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0).assertContentEquals(expected, "UTF-8");
    }

    @Test
    public void testPasswordPdfExtraction() throws FileNotFoundException {
        String expected = "\ntest\n\n\n";
        InputStream is = getFileStream("test-password-test.pdf");
        runner.setProperty(ConvertDocumentToText.PDF_PASSWORD, "test");
        runner.enqueue(is);
        runner.run();
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0).assertContentEquals(expected, "UTF-8");
    }

    // This file type works.  However, it comes with a TON of textual baggage so we can't just test
    // to see if the resulting output equals the expected text in this code.  We have to test if the
    // resulting output CONTAINS the expected text.  But, we can't use getData() because that's private
    // to the MockFlowFile interface.  However, again, We can use IntelliJ's debugger and expression parser
    // to check it though.
//    @Test
//    public void testFodtExtraction() throws FileNotFoundException {
//        String expected = "This is test text.";
//        InputStream is = getFileStream("test-flat-xml-odf.fodt");
//        runner.enqueue(is);
//        runner.run();
//        runner.assertTransferCount("success", 1);
//        runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0).assertContentEquals(expected, "UTF-8");
//        String result = new String(runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0).getData());
//        assertTrue(result.contains("This is test text."));
//    }

    @Test
    public void testOdfExtraction() throws FileNotFoundException {
        String expected = "test\n";
        InputStream is = getFileStream("test-odf.odt");
        runner.enqueue(is);
        runner.run();
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0)
                .assertContentEquals(expected, "UTF-8");
    }

    @Test
    public void testOoXmlExtraction() throws FileNotFoundException {
        String expected = "test\n";
        InputStream is = getFileStream("test-ooxml.docx");
        runner.enqueue(is);
        runner.run();
        runner.assertTransferCount("success", 1);
        runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0)
                .assertContentEquals(expected, "UTF-8");
    }

    // This file type works.  However, it comes with a TON of textual baggage so we can't just test
    // to see if the resulting output equals the expected text in this code.  We have to test if the
    // resulting output CONTAINS the expected text.  But, we can't use getData() because that's private
    // to the MockFlowFile interface.  However, again, We can use IntelliJ's debugger and expression parser
    // to check it though.
//    @Test
//    public void testWord2003Extraction() throws FileNotFoundException {
//        String expected = "test\n";
//        InputStream is = getFileStream("test-word-2003.xml");
//        runner.enqueue(is);
//        runner.run();
//        runner.assertTransferCount("success", 1);
//        runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0).assertContentEquals(expected, "UTF-8");
//        String result = new String(runner.getFlowFilesForRelationship(ConvertDocumentToText.REL_SUCCESS).get(0).getData());
//        assertTrue(result.contains("This is test text."));
//    }

    private InputStream getFileStream(String relFilePath) throws FileNotFoundException
    {
        ClassLoader loader = Thread.currentThread()
                .getContextClassLoader();
        URL sctUrl = loader.getResource(relFilePath);
        if (sctUrl == null) {
            throw new RuntimeException("Bad test file path [" + relFilePath + "]");
        }
        return new FileInputStream(sctUrl.getFile());
    }


}
