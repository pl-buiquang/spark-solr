# Publishing to Maven Central

This document describes how to publish releases of this artifact to Maven Central using GitHub Actions.

## Prerequisites

1. **Maven Central Account**: You should have registered your namespace `io.github.pl-buiquang` with [central.sonatype.com](https://central.sonatype.com)
2. **GPG Key**: You need a GPG key pair for signing artifacts
3. **GitHub Repository**: The code should be hosted on GitHub

## Setup GitHub Secrets

Configure the following secrets in your GitHub repository (Settings → Secrets and variables → Actions):

### Required Secrets

1. **`OSSRH_USERNAME`**: Your Sonatype OSSRH username (same as central.sonatype.com login)
2. **`OSSRH_PASSWORD`**: Your Sonatype OSSRH password or user token
3. **`GPG_PRIVATE_KEY`**: Your GPG private key in ASCII-armored format
4. **`GPG_PASSPHRASE`**: The passphrase for your GPG private key

### How to Get GPG Key Information

1. **Generate a GPG key pair** (if you don't have one):
   ```bash
   gpg --gen-key
   ```

2. **Export your GPG private key**:
   ```bash
   gpg --armor --export-secret-keys YOUR_EMAIL > private-key.asc
   ```
   Copy the contents of `private-key.asc` to the `GPG_PRIVATE_KEY` secret.

3. **Upload your public key to key servers**:
   ```bash
   gpg --keyserver keyserver.ubuntu.com --send-keys YOUR_KEY_ID
   gpg --keyserver keys.openpgp.org --send-keys YOUR_KEY_ID
   ```

## Publishing Methods

### Method 1: GitHub Release (Recommended)

1. Create a new release on GitHub:
   - Go to "Releases" → "Create a new release"
   - Tag version: `v4.1.0` (or your desired version)
   - Release title: `Release 4.1.0`
   - Click "Publish release"

2. The GitHub Action will automatically:
   - Set the version from the tag
   - Run tests
   - Build and sign artifacts
   - Upload to Maven Central

### Method 2: Manual Workflow Dispatch

1. Go to "Actions" → "Publish to Maven Central"
2. Click "Run workflow"
3. Enter the version number (e.g., `4.1.0`)
4. Click "Run workflow"

## Workflow Details

The GitHub Actions workflow (`/.github/workflows/publish.yml`) performs these steps:

1. **Setup**: Checkout code, setup JDK 11, cache Maven dependencies
2. **GPG**: Import GPG key for artifact signing
3. **Maven**: Configure Maven settings with OSSRH credentials
4. **Version**: Set the version based on release tag or manual input
5. **Test**: Run the test suite to ensure quality
6. **Deploy**: Build, sign, and upload artifacts to Maven Central
7. **Tag**: Create and push git tag (for manual workflow only)

## Artifact Information

The published artifacts will be available at:

- **Group ID**: `io.github.pl-buiquang`
- **Artifact ID**: `spark-solr`
- **Repository**: https://s01.oss.sonatype.org/ (releases) and https://repo1.maven.org/maven2/ (central)

## Maven Coordinates

Users can include your library in their projects using:

### Maven
```xml
<dependency>
    <groupId>io.github.pl-buiquang</groupId>
    <artifactId>spark-solr</artifactId>
    <version>4.1.0</version>
</dependency>
```

### Gradle
```gradle
implementation 'io.github.pl-buiquang:spark-solr:4.1.0'
```

### SBT
```scala
libraryDependencies += "io.github.pl-buiquang" % "spark-solr" % "4.1.0"
```

## Troubleshooting

### Common Issues

1. **GPG Signing Fails**: Ensure your GPG_PRIVATE_KEY is properly formatted and GPG_PASSPHRASE is correct
2. **Authentication Fails**: Verify OSSRH_USERNAME and OSSRH_PASSWORD are correct
3. **Version Already Exists**: You cannot republish the same version; increment the version number
4. **Tests Fail**: The workflow will not publish if tests fail; fix tests first

### Manual Local Testing

You can test the publishing process locally:

```bash
# Set up environment variables
export GPG_TTY=$(tty)
export OSSRH_USERNAME="your-username"
export OSSRH_PASSWORD="your-password"

# Test build and sign
mvn clean package -Prelease

# Test deploy (remove -DskipTests after testing)
mvn clean deploy -Prelease -DskipTests
```

## Release Process Checklist

- [ ] Update version in README/documentation if needed
- [ ] Ensure all tests pass locally
- [ ] Commit and push all changes
- [ ] Create GitHub release with proper tag (e.g., v4.1.0)
- [ ] Verify artifacts appear in Maven Central (may take 10-30 minutes)
- [ ] Test the published artifact in a separate project

## Security Notes

- Never commit credentials or GPG keys to the repository
- Use GitHub repository secrets for all sensitive information
- Consider using Dependabot to keep your dependencies updated
- The GPG key should be dedicated to this project and rotated periodically