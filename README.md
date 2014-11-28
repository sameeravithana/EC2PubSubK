EC2PubSubK
==========

Cloud Top-k publish/subscribe

This prototype will be used to demonstrate the research project Top-k pub/sub, You can view the progress at http://ec2kpubsub-dev.elasticbeanstalk.com/

To Build
Import as Eclipse project
Set your own credential File with Amazon AWS

Including Features:
- Publication stream: simulated using Amazon Kinesis: a real world data stream processing platform
- Indexed personalized subscription spaces: implemented on top of Amazon Elastic cache (not yet): a widely adopted in-memory object caching system
- Sliding window Top-k matching with indexed publications: implemented on top of Amazon Elastic Compute Cloud (EC2) worker instances which provide re-sizable compute capacity in the cloud 
- Event delivery: implemented on top of SNS: a fast, exible, fully managed push messagiservice. 
- Persistent notication service: implemented on top of Amazon Simple Queue Service (SQS): a hosted queue for storing messages as they travel between dierent parties

To be continued:
- Apply MAXDIVREL : propsed result diversficiation method supported by the randomized algorithm Locality Sensitive Hashing (LSH), instead it can support incremental top-k computation on continuos sliding windows under a pre-defined success probability
