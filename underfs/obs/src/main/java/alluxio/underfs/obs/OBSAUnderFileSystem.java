package alluxio.underfs.obs;

import alluxio.AlluxioURI;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.util.UnderFileSystemUtils;
import com.google.common.base.Preconditions;
import com.obs.services.ObsClient;
import com.obs.services.ObsConfiguration;

import java.io.IOException;
import java.io.OutputStream;

public class OBSAUnderFileSystem extends OBSUnderFileSystem {

  /**
   * Huawei OBS client.
   */
  private final ObsClient mClient;

  /**
   * Bucket name of user's configured Alluxio bucket.
   */
  private final String mBucketName;

  /**
   * Constructs a new instance of {@link OBSUnderFileSystem}.
   *
   * @param uri  the {@link AlluxioURI} for this UFS
   * @param conf the configuration for this UFS
   * @return the created {@link OBSUnderFileSystem} instance
   */
  public static OBSAUnderFileSystem createInstance(
      AlluxioURI uri,
      UnderFileSystemConfiguration conf) throws Exception {
    Preconditions.checkArgument(
        conf.containsKey(OBSPropertyKey.OBS_ACCESS_KEY),
        "Property " + OBSPropertyKey.OBS_ACCESS_KEY + " is required to connect to OBS");
    Preconditions.checkArgument(
        conf.containsKey(OBSPropertyKey.OBS_SECRET_KEY),
        "Property " + OBSPropertyKey.OBS_SECRET_KEY + " is required to connect to OBS");
    Preconditions.checkArgument(
        conf.containsKey(OBSPropertyKey.OBS_ENDPOINT),
        "Property " + OBSPropertyKey.OBS_ENDPOINT + " is required to connect to OBS");
    String accessKey = conf.getValue(OBSPropertyKey.OBS_ACCESS_KEY);
    String secretKey = conf.getValue(OBSPropertyKey.OBS_SECRET_KEY);
    String endPoint = conf.getValue(OBSPropertyKey.OBS_ENDPOINT);
    ObsConfiguration obsConfiguration = new ObsConfiguration();
    obsConfiguration.setEndPoint(endPoint);
    obsConfiguration.setHttpsOnly(false);
    ObsClient obsClient = new ObsClient(accessKey, secretKey, obsConfiguration);
    String bucketName = UnderFileSystemUtils.getBucketName(uri);
    return new OBSAUnderFileSystem(uri, obsClient, bucketName, conf);
  }

  /**
   * Constructor for {@link OBSAUnderFileSystem}.
   *
   * @param uri        the {@link AlluxioURI} for this UFS
   * @param obsClient  Huawei OBS client
   * @param bucketName bucket name of user's configured Alluxio bucket
   * @param conf       configuration for this UFS
   */
  protected OBSAUnderFileSystem(
      AlluxioURI uri, ObsClient obsClient, String bucketName,
      UnderFileSystemConfiguration conf) {
    super(uri, obsClient, bucketName, conf);
    this.mBucketName = bucketName;
    this.mClient = obsClient;
  }

  @Override
  public String getUnderFSType() {
    return "obsa";
  }

  @Override
  public boolean supportsFlush() {
    return true;
  }

  @Override
  protected OutputStream createObject(String key) throws IOException {
    return new OBSAOutputStream(mBucketName, key, mClient);
  }
}
