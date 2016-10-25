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

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.tika.parser.PasswordProvider;
import org.xml.sax.SAXException;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypeException;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.AutoDetectParser;
// import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.sax.BodyContentHandler;

@Tags({"pdf", "doc", "tika", "attributes", "text"})
@CapabilityDescription("Convert PDF files, Word documents, etc. to plain text")
@SeeAlso(classNames = {"org.apache.nifi.processors.standard.IdentifyMimeType"})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ConvertDocumentToText extends AbstractProcessor {

    private TikaConfig config;
    private Detector detector;

    public static final PropertyDescriptor MAX_FILE_SIZE = new PropertyDescriptor
            .Builder().name("Maximum File Size")
            .displayName("Maximum File Size")
            .description("Maximum size of file to try to convert")
            .required(true)
            .defaultValue("1MB")
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .build();

    public static final PropertyDescriptor PDF_PASSWORD = new PropertyDescriptor.Builder()
            .name("PDF Password")
            .displayName("PDF Password")
            .description("The password for the PDF, if needed")
            .required(false)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description(
                "Any FlowFile that is successfully parsed is routed to " +
                "this relationship"
            )
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                "Any FlowFile that fails to be parsed is routed to " +
                "this relationship"
            )
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description(
                "The original file is always routed to this relationship"
            )
            .build();

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.config = TikaConfig.getDefaultConfig();
        this.detector = config.getDetector();

        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(MAX_FILE_SIZE);
        descriptors.add(PDF_PASSWORD);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final int maxFileSize = context.getProperty(MAX_FILE_SIZE).asDataSize(DataUnit.B).intValue();
        final long fileSize = flowFile.getSize();

        if (fileSize > maxFileSize) {
            getLogger().error("FlowFile {} file size {} exceeds maximum file size {}", new Object[]{flowFile, fileSize, maxFileSize});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final BodyContentHandler parserHandler = new BodyContentHandler(maxFileSize > 0 ? maxFileSize : -1);
        final ParseContext parserContext = new ParseContext();

        if (context.getProperty(PDF_PASSWORD).getValue() != null && !"".equals(context.getProperty(PDF_PASSWORD).getValue())) {
            parserContext.set(PasswordProvider.class, new PasswordProvider() {
                @Override
                public String getPassword(Metadata metadata) {
                    return context.getProperty(PDF_PASSWORD).getValue();
                }
            });
        }

        final AutoDetectParser parser = new AutoDetectParser();

        final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());

        final AtomicReference<String> mimeTypeRef =
            new AtomicReference<String>(null);
        final AtomicReference<Map<String, String>> attributesRef =
            new AtomicReference<Map<String, String>>(null);

        final AtomicReference<FlowFile> textRef =
            new AtomicReference<FlowFile>(session.clone(flowFile));
        final AtomicBoolean failedRef = new AtomicBoolean(false);

        session.read(flowFile, new InputStreamCallback() {
            @Override
            public void process(final InputStream stream) throws IOException {
                try (final InputStream in = new BufferedInputStream(stream)) {
                    final TikaInputStream tikaStream = TikaInputStream.get(in);
                    final Metadata metadata = new Metadata();

                    // Add filename if it exists
                    if (filename != null) {
                        metadata.add(TikaMetadataKeys.RESOURCE_NAME_KEY, filename);
                    }

                    // Get MIME type
                    final MediaType mediatype = detector.detect(tikaStream, metadata);
                    final String mimeType = mediatype.toString();

                    // Get MIME extension
                    String mimeExt = null;
                    try {
                        mimeExt = config.getMimeRepository().forName(mimeType).getExtension();
                        if (mimeExt == null || mimeExt.isEmpty()) {
                            getLogger().warn("MIME type extension is {}", new Object[]{mimeExt == null ? "null" : "empty"});
                        }
                    } catch (MimeTypeException e) {
                        getLogger().warn(e.getMessage(), e);
                    }

                    // Get document metadata
                    try {
                        parser.parse(tikaStream, parserHandler, metadata, parserContext);
                    }
                    catch (TikaException e) {
                        getLogger().error(e.getMessage(), e);
                        failedRef.set(true);
                        return;

                    }
                    catch (SAXException e) {
                        getLogger().error(e.getMessage(), e);
                        failedRef.set(true);
                        return;
                    }

                    final Map<String, String> attributes = new HashMap<String, String>();
                    for (final String key : metadata.names()) {
                        final String value = metadata.get(key);
                        if (value != null && !value.isEmpty()) {
                            attributes.put(key, value);
                        }
                    }
                    attributesRef.set(attributes);

                    FlowFile text = textRef.get();
                    text = session.putAttribute(text, CoreAttributes.MIME_TYPE.key(), mimeType);
                    text = session.putAttribute(text, "mime.extension", mimeExt);
                    if (filename != null && !filename.isEmpty() && mimeExt != null && !mimeExt.isEmpty()) {
                        text = session.putAttribute(text, CoreAttributes.FILENAME.key(), filename.replaceAll(mimeExt, ".txt"));
                    }
                    text = session.putAllAttributes(text, attributes);

                    // Write plain text to FlowFile contents
                    final FlowFile _text = text;
                    text = session.write(text, new OutputStreamCallback() {
                        @Override
                        public void process(final OutputStream stream) throws IOException {
                            try (final OutputStream out = new BufferedOutputStream(stream)) {
                                final byte[] bytes = parserHandler.toString().getBytes();
                                if (bytes.length > 0) {
                                    out.write(bytes);
                                }
                                else {
                                    getLogger().warn("FlowFile {} was empty or can't be converted to text", new Object[]{_text});
                                    failedRef.set(true);
                                }
                            }
                        }
                    });

                    textRef.set(text);
                }
            }
        });

        final FlowFile text = textRef.get();
        final boolean failed = failedRef.get();

        if (failed) {
            session.transfer(flowFile, REL_FAILURE);
            session.remove(text);
            return;
        }

        session.transfer(flowFile, REL_ORIGINAL);
        session.transfer(text, REL_SUCCESS);
    }
}
