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

import org.xml.sax.SAXException;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.InputStreamCallback;
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

@Tags({"pdf", "tika", "attributes"})
@CapabilityDescription("Extract metadata from PDF files, Word documents, etc.")
@SeeAlso(classNames = {"org.apache.nifi.processors.standard.IdentifyMimeType"})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ExtractDocumentMetadata extends AbstractProcessor {

    private TikaConfig config;
    private Detector detector;

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

        final BodyContentHandler parserHandler = new BodyContentHandler();
        final ParseContext parserContext = new ParseContext();
        final AutoDetectParser parser = new AutoDetectParser();

        final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());

        final AtomicReference<String> mimeTypeRef =
            new AtomicReference<String>(null);
        final AtomicReference<Map<String, String>> attributesRef =
            new AtomicReference<Map<String, String>>(null);
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
                    mimeTypeRef.set(mimeType);

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
                }
            }
        });

        final boolean failed = failedRef.get();
        if (failed) {
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        final String mimeType = mimeTypeRef.get();
        final Map<String, String> attributes = attributesRef.get();

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

        FlowFile copy = session.clone(flowFile);
        copy = session.putAttribute(copy, CoreAttributes.MIME_TYPE.key(), mimeType);
        copy = session.putAttribute(copy, "mime.extension", mimeExt);
        copy = session.putAllAttributes(copy, attributes);

        session.transfer(flowFile, REL_ORIGINAL);
        session.transfer(copy, REL_SUCCESS);
    }
}
