package org.broadinstitute.http.nio;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;

public class MockedIntegrationTest extends BaseTest {

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

        final UrlPattern fileUrl = urlEqualTo("/file.txt");
        final String body = "Hello";
        wireMockServer.stubFor(get(fileUrl)
                .willReturn(ok(body)));
        wireMockServer.stubFor(head(fileUrl).willReturn(ok().withHeader("content-length", String.valueOf(body.getBytes(StandardCharsets.UTF_8).length))));

        final Path path = Paths.get(getUri("/file.txt"));
        Files.exists(path);
        verify(headRequestedFor(urlMatching("/file.txt")));
        Assert.assertEquals(Files.readString(path), body);
    }

    @DataProvider
    public Object[][] getFaults(){
        return new Object[][] {
                {Fault.CONNECTION_RESET_BY_PEER},
                {Fault.EMPTY_RESPONSE},
                {Fault.RANDOM_DATA_THEN_CLOSE}
        };
    }
    @Test(dataProvider = "getFaults")
    public void testConnectionReset(Fault fault) throws IOException {
        final String body = "Hello";
        final UrlPattern fileUrl = urlEqualTo("/file.txt");
        wireMockServer.stubFor(get(fileUrl)
                .willReturn(aResponse().withFault(fault)));
//        wireMockServer.stubFor(head(fileUrl).willReturn(ok().withHeader("content-length", String.valueOf(body.getBytes(StandardCharsets.UTF_8).length))));

        try(SeekableByteChannel chan = new HttpSeekableByteChannel(getUri("/file.txt"))){
            //
        }
    }


    @Test


    URI getUri(String path){
        try{
            return new URI(wireMockServer.url(path));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

}

