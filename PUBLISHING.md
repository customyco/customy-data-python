# Publishing

Registry: **PyPI**. Canonical version: **0.1.2**.

The release workflow is tag-bound, verifies conformance and version alignment, and uses a protected GitHub environment. Publishing is considered successful only after the official registry resolves the exact version; a GitHub tag or release is not sufficient.

External one-time setup: Create a pending PyPI trusted publisher for customyco/customy-data-python, workflow publish.yml and environment pypi.

After the registry-side setup is complete, dispatch `.github/workflows/publish.yml` against the immutable `v0.1.2` tag when the registry supports manual dispatch. pub.dev requires a new matching tag push. Never replace a published version or bypass a failed conformance job.
