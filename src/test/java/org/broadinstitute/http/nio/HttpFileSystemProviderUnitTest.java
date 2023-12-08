package org.broadinstitute.http.nio;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author Daniel Gomez-Sanchez (magicDGS)
 */
public class HttpFileSystemProviderUnitTest extends BaseTest {

    @Test
    public void testGetScheme() {
        Assert.assertEquals(new HttpFileSystemProvider().getScheme(), "http");
    }
}
