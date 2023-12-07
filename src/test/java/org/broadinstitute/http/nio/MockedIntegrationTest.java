package org.broadinstitute.http.nio;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;

public class MockedIntegrationTest extends BaseTest {

    public static final UrlPattern FILE_URL = urlEqualTo("/file.txt");
    public static final String BODY = "Hello";
    WireMockServer wireMockServer;
    WireMock wireMock;

    @BeforeMethod
    void start(){
        wireMockServer = new WireMockServer(WireMockConfiguration
                .wireMockConfig()
                .dynamicPort());
        wireMockServer.start();
        configureFor("localhost", wireMockServer.port());
        wireMock = new WireMock("localhost", wireMockServer.port());
    }
    @AfterMethod
    void stop(){
        wireMockServer.stop();
    }

    @Test
    public void testMock() throws IOException {

        wireMockServer.stubFor(get(FILE_URL)
                .willReturn(ok(BODY)));
        wireMockServer.stubFor(head(FILE_URL)
                .willReturn(ok()
                .withHeader("content-length", String.valueOf(BODY.getBytes(StandardCharsets.UTF_8).length))));

        final Path path = Paths.get(getUri("/file.txt"));
        Files.exists(path);
        verify(headRequestedFor(urlMatching("/file.txt")));
        Assert.assertEquals(Files.readString(path), BODY);
    }

    @DataProvider
    public Object[][] getFaults(){
        return new Object[][] {
                {Fault.CONNECTION_RESET_BY_PEER},
                {Fault.EMPTY_RESPONSE},
                {Fault.RANDOM_DATA_THEN_CLOSE}
        };
    }
    @Test(dataProvider = "getFaults", expectedExceptions = OutOfRetriesException.class)
    public void testConnectionReset(Fault fault) throws IOException {
        final String body = "Hello";
        final UrlPattern fileUrl = urlEqualTo("/file.txt");
        wireMockServer.stubFor(get(fileUrl)
                .willReturn(aResponse().withFault(fault)));
        try(SeekableByteChannel chan = new HttpSeekableByteChannel(getUri("/file.txt"))){
            //this should fail with the appropriate exception
        }
    }


    @Test
    public void testRetryFixesError() throws IOException {
        final String body = "Hello";
        wireMockServer.stubFor(get(FILE_URL).inScenario("fail once")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER))
                .willSetStateTo("errored"));

        final long bodyLength = body.getBytes(StandardCharsets.UTF_8).length;
        wireMockServer.stubFor(get(FILE_URL).inScenario("fail once")
                .whenScenarioStateIs("errored")
                .willReturn(ok(body).withHeader("content-length", String.valueOf(bodyLength)))
                .willSetStateTo(Scenario.STARTED));

        wireMockServer.stubFor(head(FILE_URL).inScenario("fail once")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(aResponse().withFault(Fault.CONNECTION_RESET_BY_PEER))
                .willSetStateTo("errored"));

        wireMockServer.stubFor(head(FILE_URL).inScenario("fail once")
                .whenScenarioStateIs("errored")
                .willReturn(ok().withHeader("content-length", String.valueOf(bodyLength)))
                .willSetStateTo(Scenario.STARTED));


        final URI uri = getUri("/file.txt");
        final HttpFileSystemProviderSettings settings = new HttpFileSystemProviderSettings(Duration.ofSeconds(2),
                HttpClient.Redirect.NORMAL,
                new HttpFileSystemProviderSettings.RetrySettings(1, RetryHandler.DEFAULT_RETRYABLE_HTTP_CODES,
                        RetryHandler.DEFAULT_RETRYABLE_EXCEPTIONS,
                        RetryHandler.DEFALT_RETRYABLE_MESSAGES,
                        e -> false));

        try(final HttpSeekableByteChannel channel = new HttpSeekableByteChannel(uri, settings, 0L) ) {
            Assert.assertEquals(channel.size(), bodyLength);
            Assert.assertEquals(channel.position(), 0);
            final ByteBuffer buf = ByteBuffer.allocate(2);
            Assert.assertEquals(channel.read(buf), 2);
            Assert.assertEquals(buf.array(), new byte[]{'H','e'});
            Assert.assertEquals(channel.position(), 2);

            channel.position(4);
        }
    }


    @DataProvider
    public static Object[][] getReturnCodes() {
        return new Object[][]{
                {200, 0, null},
                {200, 100, IncompatibleResponseToRangeQueryException.class},
                {206, 100, null},
                {206, 0, IncompatibleResponseToRangeQueryException.class},
                {404, 0, FileNotFoundException.class},
                {500, 0, OutOfRetriesException.class},
                {700, 0, UnexpectedHttpResponseException.class},
        };
    }

    @Test(dataProvider = "getReturnCodes")
    public void testSpecialReturnCodes(int response, int position, Class<? extends Exception> expectedException){
        wireMockServer.stubFor(get(FILE_URL)
                .willReturn(aResponse().withStatus(response)));
        if(expectedException != null){
            Assert.assertThrows(expectedException, () -> {
                try(SeekableByteChannel channel = new HttpSeekableByteChannel( getUri("/file.txt"), position)){
                    //shouldn't get here
                }
            });
        }
    }

    URI getUri(String path){
        try{
            return new URI(wireMockServer.url(path));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

}

