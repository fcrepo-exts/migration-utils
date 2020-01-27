/*
 * Copyright 2015 DuraSpace, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.fcrepo.migration.foxml;

import org.apache.commons.io.FileUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * An InternalIDResolver implementation that generates an index of
 * datastream ids (filenames) to file paths for the contents of a
 * datastream directory.  The directory is expected to contain just
 * other directories and/or FOXML files.  The FOXML files are expected
 * to have a filename that is reversibly mapped from a fedora internal
 * id for that datastream version.
 * @author mdurbin
 */
public abstract class DirectoryScanningIDResolver implements InternalIDResolver {

    private static final Logger LOGGER = getLogger(InternalIDResolver.class);

    /**
     * A lucene IndexSearcher over an index maintained by this class.
     * For every file found in the datastream directory a document exists
     * in this index that contains an "id" field and a "path" field.  The
     * id field is the internal id, the path field is the full path to the
     * file containing that datastream content.
     */
    private IndexSearcher searcher;

    /**
     * directory scanning ID resolver
     * @param cachedIndexDir the index directory.  If it exists, the old cache will be used, if it doesn't a new
     *                 cache will be built at that location.  If it is null, a new cache will be built in
     *                 the temp file space that will be deleted upon application shutdown.
     * @param dsRoot the datastream root
     * @throws IOException IO exception
     */
    public DirectoryScanningIDResolver(final File cachedIndexDir, final File dsRoot) throws IOException {
        final File indexDir;
        if (cachedIndexDir == null) {
            final File temp = File.createTempFile("tempfile", "basedir");
            temp.delete();
            temp.mkdir();
            indexDir = new File(temp, "index");
            LOGGER.info("No index directory specified.  Creating temporary index at \""
                    + indexDir.getAbsolutePath() + "\".");
            Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        LOGGER.info("Deleting index directory at \"" + indexDir.getAbsolutePath() + "\"...");
                        FileUtils.deleteDirectory(indexDir);
                    } catch (IOException e) {
                        LOGGER.error("Unable to delete index directory at \"" + indexDir.getAbsolutePath() + "\"!", e);
                        e.printStackTrace();
                    }
                }
            }));
        } else {
            indexDir = cachedIndexDir;
        }

        // Index dir exists and is non-empty
        if (indexDir.exists() && indexDir.list().length > 0) {
            LOGGER.warn("Index exists at \"" + indexDir.getPath() + "\" and will be used.  "
                    + "To clear index, simply delete this directory and re-run the application.");
        } else {
            final Analyzer analyzer = new StandardAnalyzer();
            final IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            final Directory dir = FSDirectory.open(indexDir.toPath());
            final IndexWriter writer = new IndexWriter(dir, iwc);
            LOGGER.info("Building an index of all the datastreams in \"" + dsRoot.getPath() + "\"...");
            indexDatastreams(writer, dsRoot);

            writer.commit();
            writer.close();
        }

        final IndexReader reader = DirectoryReader.open(FSDirectory.open(indexDir.toPath()));
        searcher = new IndexSearcher(reader);
    }

    @Override
    public CachedContent resolveInternalID(final String id) {
        try {
            final TopDocs result = searcher.search(new TermQuery(new Term("id", id)), 2);
            if (result.totalHits == 1) {
                return new FileCachedContent(new File(searcher.doc(result.scoreDocs[0].doc).get("path")));
            } else if (result.totalHits < 1) {
                throw new RuntimeException("Unable to resolve internal ID \"" + id + "\"!");
            } else {
                throw new IllegalStateException(result.totalHits + " files matched the internal id \"" + id + "\".  ("
                        + searcher.doc(result.scoreDocs[0].doc).get("path") + ", "
                        + searcher.doc(result.scoreDocs[1].doc).get("path") + "...)");
            }
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void indexDatastreams(final IndexWriter writer, final File f) throws IOException {
        if (f.isDirectory()) {
            for (final File child : f.listFiles()) {
                indexDatastreams(writer, child);
            }
        } else {
            final Document doc = new Document();
            doc.add(new StringField("path", f.getPath(), Field.Store.YES));
            doc.add(new StringField("id", getInternalIdForFile(f), Field.Store.YES));
            LOGGER.trace("Added \"{}\" for: {}", getInternalIdForFile(f), f.getPath());
            writer.addDocument(doc);
        }
    }

    /**
     * Determines the internal id for the given file.
     *
     * @param f file to check for
     * @return string containing internal id for the file
     */
    protected abstract String getInternalIdForFile(File f);

}
