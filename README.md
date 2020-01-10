# GDC MuSE
![Version badge](https://img.shields.io/badge/MuSE-v1.0rc__submission__c039ffa-<COLOR>.svg)

MuSE is somatic point mutation caller developed by Dr. Wenyi Wang’s group in MD Anderson Cancer Center (MDACC), and used by the Human Genome Sequencing Center (HGSC) in the Baylor College of Medicine (BCM). Thus it is often referred to as the Baylor pipeline. This pipeline takes a pair of tumor and normal BAM files, and does two-step calling and summarization to generate somatic SNV and INDEL VCFs. The only auxiliary file requirement is a VCF file from Single Nucleotide Polymorphism Database (dbSNP) for annotation.

Original MuSE: http://bioinformatics.mdanderson.org/main/MuSE

## Docker

There are three `Dockerfile`s for different purposes:

* Vanilla MuSE
  * `/docker/muse/Dockerfile` : MuSE docker without additional features.
* MuSE call merge
  * `/docker/muse_merge/Dockerfile` : A simple python code that can merge MuSE call outputs. It can be considered as `gather` part when applying `scatter/gather` on MuSE.
* Multi-threading MuSE call
  * `/docker/multi_muse/Dockerfile` : A python multi-threading implementation on MuSE call function. Achieve `scatter/gather` method on Docker level.

## How to build

https://docs.docker.com/engine/reference/builder/

The docker images are tested under multiple environments. The most tested ones are:
* Docker version 19.03.2, build 6a30dfc
* Docker version 18.09.1, build 4c52b90
* Docker version 18.03.0-ce, build 0520e24
* Docker version 17.12.1-ce, build 7390fc6

## For external users

There is a production-ready CWL example at https://github.com/NCI-GDC/muse-cwl which uses the docker images that are built from the `Dockerfile`s in this repo.

To use docker images directly or with other workflow languages, we recommend to build and use either vanilla MuSE or multi-threading MuSE call.

To run multi-threading MuSE call:

```
[INFO] [20200109 02:19:20] [multi_muse_call] - --------------------------------------------------------------------------------
[INFO] [20200109 02:19:20] [multi_muse_call] - multi_muse_call.py
[INFO] [20200109 02:19:20] [multi_muse_call] - Program Args: docker/multi_muse/multi_muse_call.py -h
[INFO] [20200109 02:19:20] [multi_muse_call] - --------------------------------------------------------------------------------
usage: Internal multithreading MuSE call. [-h] -f REFERENCE_PATH -r
                                          INTERVAL_BED_PATH -t TUMOR_BAM -n
                                          NORMAL_BAM -c THREAD_COUNT

optional arguments:
  -h, --help            show this help message and exit
  -f REFERENCE_PATH, --reference_path REFERENCE_PATH
                        Reference path.
  -r INTERVAL_BED_PATH, --interval_bed_path INTERVAL_BED_PATH
                        Interval bed file.
  -t TUMOR_BAM, --tumor_bam TUMOR_BAM
                        Tumor bam file.
  -n NORMAL_BAM, --normal_bam NORMAL_BAM
                        Normal bam file.
  -c THREAD_COUNT, --thread_count THREAD_COUNT
                        Number of thread.
```

## For GDC users

See https://github.com/NCI-GDC/gdc-somatic-variant-calling-workflow.
