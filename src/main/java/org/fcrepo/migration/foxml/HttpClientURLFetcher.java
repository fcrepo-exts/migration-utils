/**
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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
/**
 *
 * @author mdurbin
 *
 */
public class HttpClientURLFetcher implements URLFetcher {

    CloseableHttpClient httpClient;

    /**
     * Http Client URL fetcher.
     */
    public HttpClientURLFetcher() {
        httpClient = HttpClients.createDefault();
    }

    @Override
    public InputStream getContentAtUrl(final URL url) throws IOException {
        return httpClient.execute(new HttpGet(String.valueOf(url))).getEntity().getContent();

    }
}
