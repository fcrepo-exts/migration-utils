package org.fcrepo.migration.foxml11;

import static org.slf4j.LoggerFactory.getLogger;

import java.io.File;
import java.io.IOException;
import java.net.URLDecoder;

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
import org.apache.lucene.util.Version;
import org.slf4j.Logger;

/**
 * An InternalIDResolver implementation that generates an index of
 * datastream ids (filenames) to file paths for the contents of a
 * datastream directory.  The directory is expected to contain just
 * other directories and/or FOXML files.  The FOXML files are expected
 * to have a filename that is reversibly mapped from a fedora internal
 * id for that datastream version.
 * @author mdurbin
 */
public class DirectoryScanningIDResolver implements InternalIDResolver {

    private static final Logger LOGGER = getLogger(InternalIDResolver.class);

    private IndexSearcher searcher;

    /**
     * directory scanning ID resolver
     * @param indexDir the index directory
     * @param dsRoot the datastream root
     * @throws IOException IO exception
     */
    public DirectoryScanningIDResolver(final File indexDir, final File dsRoot) throws IOException {
        final Directory dir = FSDirectory.open(indexDir);
        if (indexDir.exists()) {
            LOGGER.warn("Index exists at \"" + indexDir.getPath() + "\" and will be used.  "
                    + "To clear index, simply delete this directory and re-run the application.");
        } else {
            final Analyzer analyzer = new StandardAnalyzer();
            final IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_4_10_3, analyzer);
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            final IndexWriter writer = new IndexWriter(dir, iwc);
            LOGGER.info("Builidng an index of all the datastreams in \"" + dsRoot.getPath() + "\"...");
            indexDatastreams(writer, dsRoot);

            writer.commit();
            writer.close();
        }

        final IndexReader reader = DirectoryReader.open(FSDirectory.open(indexDir));
        searcher = new IndexSearcher(reader);
    }

    @Override
    public CachedContent resolveInternalID(final String id) {
        try {
            final TopDocs result = searcher.search(new TermQuery(new Term("file", "info:fedora/"
                    + id.replace('+', '/'))), 2);
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
            doc.add(new StringField("file", URLDecoder.decode(f.getName(), "UTF-8"), Field.Store.NO));
            writer.addDocument(doc);
        }
    }

}
