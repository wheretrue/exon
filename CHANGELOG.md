# CHANGELOG

## v0.19.0 (2024-04-23)

### Feat

- add read_cram and read_fcs methods (#487)

## v0.18.0 (2024-04-22)

### Feat

- simplify genbank schema (#485)

## v0.17.0 (2024-04-22)

### Feat

- simplify mzml reader

## v0.16.7 (2024-04-22)

### Refactor

- use ExonArrayBuilder for bcf (#484)

## v0.16.6 (2024-04-18)

### Fix

- various fixes for biobear side (#481)

## v0.16.5 (2024-04-18)

### Fix

- update noodles

## v0.16.4 (2024-04-17)

### Fix

- re-export ExonError

## v0.16.3 (2024-04-17)

### Fix

- fix release script

## v0.16.2 (2024-04-17)

### Refactor

- improve config for motif-udfs

## v0.16.1 (2024-04-16)

### Fix

- fix featurization

## v0.16.0 (2024-04-16)

### Feat

- bigwig partition and method (#473)
- add bigwig value (#472)
- region filtering for bigwig zoom (#471)
- zoom table scan (#470)
- initial bigwig batch reader (#469)
- add integer_encoding (#466)
- support gff3 extensions (#454)

### Refactor

- move bigwig zoom value to new setup (#477)
- simplify options (#476)

## v0.15.0 (2024-03-27)

### Feat

- add regex positional match (#453)
- support options for file extension in fasta and fastq (#448)
- **cram**: use reference if available
- **cram**: return reference name vs stringified id (#446)
- **cram**: support all fields (#443)
- basic cram execution (#442)

## v0.14.0 (2024-03-14)

### Feat

- update exon to v13 (#437)

### Fix

- remove rust bio dep (#439)

## v0.13.0 (2024-03-10)

### Feat

- **exonr**: updating exonr version (#435)

## v0.12.0 (2024-03-10)

### Feat

- ic for alignment score (#433)
- support local gzipped index (#432)
- better reverse_complement (#431)
- add polya trimming (#428)
- add two new UDFs (#427)

### Fix

- remove unwraps

## v0.11.1 (2024-03-03)

### Feat

- support s3 region files (#420)
- update (#418)
- working with region file (#415)

### Fix

- fix tests (#421)

### Refactor

- cleanup for point release (#422)

## v0.11.0 (2024-02-28)

### Feat

- add more code to python (#414)
- move python code into proper (#413)
- add better read_fastq and read_fasta methods (#412)

## v0.10.0 (2024-02-25)

### Feat

- Add basic indexed FASTA (#409)

## v0.9.3 (2024-02-08)

## v0.9.2 (2024-02-08)

### Feat

- handle extra semiquote in gff (#399)

### Refactor

- **exonr**: use error (#396)

## v0.9.1 (2024-02-07)

## v0.9.0 (2024-02-07)

### Feat

- more datatypes (#390)
- update fastx readers for speed (#388)
- remove justfile (#382)
- basic support for inferred BAM tag fields (#381)
- support large strings (#377)

### Fix

- fix ignore on test (#378)

### Refactor

- use dedicated struct of bgzf offsets (#385)
- move save out of exon-core (#379)

## v0.8.6 (2024-01-29)

### Refactor

- remove vcf unwraps (#373)

## v0.8.5 (2024-01-28)

### Fix

- fix issue parsing broad file (#372)
- update tests (#371)

### Refactor

- remove some unwraps (#370)

## v0.8.4 (2024-01-27)

### Fix

- support bgzip for fastq (#368)
- fix settings extractions (#367)

## v0.8.3 (2024-01-26)

### Fix

- update BAM/SAM tests (#366)

## v0.8.2 (2024-01-25)

## v0.8.1 (2024-01-25)

## v0.8.0 (2024-01-23)

### Feat

- register copy to target
- vcf improvements

### Refactor

- update for df 35

## v0.7.1 (2024-01-11)

## v0.7.0 (2024-01-10)

### Feat

- improve s3 handling (#349)
- better nextflow setup (#348)

### Fix

- add missing feature

## v0.6.0 (2024-01-02)

### Feat

- add indexed gff table (#335)
- indexed gff file

### Fix

- fix path

## v0.5.5 (2023-12-17)

### Feat

- support additional file types
- add indexed vcf
- add indexed bam
- add bcf scan function
- add vcf
- add sam scan fun
- bam udf
- add mzml scan function
- add fcs scan
- add genbank scan
- add hmm_dom_tab_scan
- add bed
- add gtf scan
- add gff scan fun
- add fastq scan fun
- add optional arg for compression

### Fix

- fix count bug
- fix up error message
- fix aws dep for exon-io

### Refactor

- no unwrap in bin vectors
- more general names
- remove bcf/vcf
- separate genbank
- refactor fcs
- pull out mzml parser
- remove DEFAULT_BATCH_SIZE
- generalize scan function
- update to new datafusion

## v0.5.4 (2023-12-13)

### Refactor

- fixup from removal
- remove exome (#326)

## v0.5.3 (2023-11-28)

## v0.5.2 (2023-11-28)

## v0.5.1 (2023-11-28)

## v0.5.0 (2023-11-28)

### Feat

- **core**: use datafusion repartitioning (#314)

### Fix

- empty projections (#313)

## v0.4.3 (2023-11-27)

### Fix

- get extension properly (#311)

## v0.4.2 (2023-11-27)

## v0.4.1 (2023-11-27)

## v0.4.0 (2023-11-27)

### Feat

- add core to bed

### Fix

- **exome**: add back tls option (#309)

## v0.3.12 (2023-11-26)

### Feat

- **exome**: tweak connection
- **exome**: add exome options (#306)

### Refactor

- move out bed (#304)
- move out gtf file reading (#303)

## v0.3.11 (2023-11-20)

### Fix

- fix bcf stats bug (#296)

## v0.3.10 (2023-11-19)

### Feat

- update df to 33 (#288)
- bump noodles
- update r version
- better exonr error handling (#282)
- add test for r to data.frame (#277)
- update exon in R package (#274)
- add exome error type (#270)
- create TableSchema (#268)
- release notes

### Fix

- dont clobber phys optimizers (#289)
- fix test syntax

### Refactor

- separate out fastq batch reader (#283)
- exome error (#275)
- add code (#271)

## v0.3.9 (2023-11-07)

### Feat

- refactor partition optimization (#263)

## v0.3.8 (2023-11-07)

### Fix

- remove union (#262)

## v0.3.8-beta.4 (2023-11-06)

## v0.3.8-beta.2 (2023-11-06)

### Feat

- add sam scan (#261)
- pushdown for bam (#260)
- update bcf pruning (#259)
- fcs projection pruning (#258)
- add hmm partition pruning (#257)
- update aws bits (#256)
- add mzml (#251)
- add bed partition pruning (#249)
- vcf partition pushdown (#246)
- refactor to file partition (#245)
- add fastq pruning tests (#244)
- refactor hive pushdown + fasta (#243)

## v0.3.6 (2023-10-30)

## v0.3.5 (2023-10-30)

### Feat

- prune gff partitions with pushdown (#241)

## v0.3.4-beta.12 (2023-10-30)

## v0.3.4-beta.11 (2023-10-30)

### Feat

- better error messages (#239)
- add partition cols to table schema (#237)

## v0.3.4 (2023-10-29)

### Feat

- cleanup docs

## v0.3.4-beta.10 (2023-10-29)

### Feat

- create statement works (#238)
- add descriptions
- updates for exome support (#234)
- start of gff hive parititoning (#231)
- update exonR for duckdb compat (#226)
- add exone-exome (#225)
- refactoring file listing (#222)
- add projection to unindexed case
- support bam repartitioning
- add vcf position tracking
- track read sequences (#205)
- update exonpy
- small tweaks
- better error messages (#203)
- add vcf region filter (#201)
- add bam pushdown (#195)
- pass down bam filter (#192)
- add tags to bam (#188)
- add start/end interval for bam/sam/cram (#186)
- update exome (#184)
- add sqllogictests for fasta/fastq (#181)
- add indexed file type (#179)
- flight con works (#177)
- separate indexed reader (#176)
- support region filters in larger tree (#175)
- connection (#164)
- setup proto for server con (#163)
- add connect method (#160)
- support table compression in exome client (#157)
- streaming bgzf vcf scans (#151)
- get next chunk (#149)
- add back query vcf method on ctx (#148)
- configurable allocation size (#142)
- add capacity (#138)
- chrom eq works (#135)
- make interval optional w/ region expr (#134)
- improve feature rewriting (#133)
- update benchmarks (#130)
- add settings plus testing (#128)
- fixup push downs (#125)
- lazy vcf parsing (#122)
- better FASTA perf, better docs (#121)
- add between rewrite (#113)
- repartition with rule (#108)
- add mzml to R (#89)
- add config with smarter defaults (#86)
- add file-parallelism to other files (#85)
- add fasta parallelism (#83)
- update df to 28
- fixes from mzml pocs (#81)
- contains peak filter (#80)
- adding initial setup for exome service (#75)
- allow some missing mzml files (#63)
- add udfs (#62)
- add mass spec binning (#60)
- fix byteorder import
- cleanup schema (#54)
- add error handling (#53)
- working fcs datasource (#50)
- add fcs reader (#47)
- early warn on bad creds
- handle path in registration
- add aws default chain (#44)
- updating versions (#42)

### Fix

- fix clippy
- include protocol for flight conn (#165)
- bgzf uncomperssed offset (#140)
- fix the benchmark (#136)
- add back info/format fields (#123)
- export mzml file
- fix genbank feature flag
- fix AWS_PROFILE (#51)
- cleanup smoke test
- fix feature tag for gcp
- correct schema

### Refactor

- pull out FASTA (#220)
- create core package
- refactor session setup
- remove BAM array builder (#191)
- update chrom expr to handle generic fields (#185)
- refactor vcf reader (#171)
- more cleanup (#150)
- add register vcf (#137)

## v0.2.3 (2023-07-05)

## v0.2.2 (2023-07-05)

## v0.2.1 (2023-07-05)

### Feat

- refactor r functions (#28)
- filling out r funcs (#27)
- add spectra cv params (#26)
- ic for R lib (#25)
- add precusor/isolation window (#24)
- adding array to mzml (#22)
- add scan and precursor lists to spectrum (#21)
- add bcf querying (#18)
- add register function on context (#17)
- add register function on context (#16)
- add gtf source (#14)
- cleanup benchmars (#13)
- add bam query (#11)
- local vcf query (#10)
- add first samflag (#8)
- gc content (#6)
- ic of udf (#5)
- add Display trait for TCAFileType
- ic

### Fix

- fix null, add benchs (#39)
- fix bug when no precursors (#30)
- add missing gtf to sources list (#23)
- dont fail on bad directive (#15)

### Refactor

- rename to exon
