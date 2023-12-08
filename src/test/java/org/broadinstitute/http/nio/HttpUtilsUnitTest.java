package org.broadinstitute.http.nio;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.URLConnection;
import java.net.http.HttpClient;

/**
 * @author Daniel Gomez-Sanchez (magicDGS)
 */
public class HttpUtilsUnitTest extends BaseTest {

    @DataProvider
    public Object[][] illegalArgumentsForRangeRequest() throws Exception {
        // create a Mocked URL connection that throws an assertion error when
        // the setRequestProperty is set
        final URLConnection mockedConnection = Mockito.mock(URLConnection.class);
        Mockito.doThrow(new AssertionError("Called setRequestProperty")).when(mockedConnection)
                .setRequestProperty(Mockito.anyString(), Mockito.anyString());

        return new Object[][] {
                // invalid start
                {mockedConnection, -1, 10},
                // invalid end
                {mockedConnection, 10, -2},
                // reverse request
                {mockedConnection, 100, 10}
        };
    }

    @Test(dataProvider = "getDocsFilesForTesting", dataProviderClass = GitHubResourcesIntegrationTest.class)
    public void testExistingUrls(final String fileName) throws IOException {
        Assert.assertTrue(HttpUtils.exists(getGithubPagesFileUri(fileName), HttpFileSystemProviderSettings.DEFAULT_SETTINGS));
    }

    @DataProvider
    public Object[][] nonExistantUrlStrings() {
        return new Object[][] {
                // unknown host
                {"http://www.doesntexist.invalid"},
                // non existant resource
                {"http://www.example.com/nonexistant.html"}
        };
    }

    // This tests fails when using verizon because they return 200 and a page with adds when you visit an unknown site
    // probably other home internet providers fail too
    @Test(dataProvider = "nonExistantUrlStrings")
    public void testNonExistingUrl(final String urlString) throws IOException {
        final URI nonExistant = URI.create(urlString);
            Assert.assertFalse(HttpUtils.exists(nonExistant, HttpFileSystemProviderSettings.DEFAULT_SETTINGS));
    }
}