package org.broadinstitute.http.nio;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.matching.UrlPattern;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;

public class MockedIntegrationTest extends BaseTest {

    WireMockServer wireMockServer;
    WireMock wireMock;

    @BeforeMethod
    void start(){
        wireMockServer = new WireMockServer(WireMockConfiguration.wireMockConfig()
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

    URI getUri(String path){
        try{
            return new URI(wireMockServer.url(path));
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

}

