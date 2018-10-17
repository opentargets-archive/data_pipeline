The build-base container gets automatically built to a quay.io repo for each tag that matches the regex `circle.*`

[![Docker Repository on Quay](https://quay.io/repository/opentargets/mrtarget_build_base/status "Docker Repository on Quay")](https://quay.io/repository/opentargets/mrtarget_build_base)

To trigger a new build after you change this image:

```sh
## on your machine
git tag circle-anotherversion
git push origin --tags
```

Then go to the config.yml file and edit it to point to the new tag. 
