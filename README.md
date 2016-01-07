# MuSE_variant_calling_pipeline
MuSE is somatic point mutation caller developed by Dr. Wenyi Wang’s group in MD Anderson Cancer Center (MDACC), and used by the Human Genome Sequencing Center (HGSC) in the Baylor College of Medicine (BCM). Thus it is often referred to as the Baylor pipeline. This pipeline takes a pair of tumor and normal BAM files, and does two-step calling and summarization to generate somatic SNV and INDEL VCFs. The only auxiliary file requirement is a VCF file from Single Nucleotide Polymorphism Database (dbSNP) for annotation.

The MuSE tool : http://bioinformatics.mdanderson.org/main/MuSE

The workflow is shown below

![Alt text](https://github.com/NCI-GDC/muse-tool/blob/develop/docs/muse_variant_calling_workflow.png "MuSE-workflow")
