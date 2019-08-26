# Identity matching in the litterature

## Most relevant papers

* [Developer identification methods for integrated data from various sources](https://drive.google.com/open?id=1RLBCELuJ3NTsw66x5yNdsQL2yJixBAgu), 2005 from Jesus M. Gonzalez-Barahona and Gregorio Robles.

    * Early work done on identity matching.
    * Approach, based on the application of heuristics, to identify the many identities of developers.
    * Cumulates identity info from different data sources: source code, versionin repo, bug tracking, mailing lists etc.
    * Evaluation on the GNOME project.
    * Tackle the privacy issues.


* [A comparison of identity merge algorithms for software repositories](https://drive.google.com/open?id=1nJdXDY6pdo-PdfFhGYMr3HU4AMqPyhIk), 2011

    * Approach very similar to ours.
    * Metrics clearly defined.
    * Description of different algorithms, some of them more complex: Bird’s algorithm, Robles’s approach + improvments.
    * Comparison of the algorithms evaluated on large open source projects.


* [Who’s who in GNOME: using LSA to merge software repository identities](https://drive.google.com/open?id=1PoDlWEzWUd1Tra2sGi0gdz8k2M-Y2U3l), 2012

    * Start the paper saying that existing identity merging algorithms are sensitive to large discrepancies between the aliases used by the same individual: the noisier the data, the worse their performance. -> Discussion more pragmatic about the noise in the data and the scale.
    * Study all GNOME Git repositories and discuss robustness of existing algorithms.
    * Propose a new identity merging algorithm based on Latent Semantic Analysis (LSA)

* [Who is who in the mailing list? Comparing six disambiguation heuristics to identify multiple addresses of a participant](https://drive.google.com/open?id=1EKcy-QCb7kunMMJkZyfhcj_00UWbMLjo), 2015

    * Benchmark of 6 heuristics (among them, the ones covered in the first benchmark from 2011) to perform identity matching, tested on the Apache projects.
    * Interesting research questions, among them one about time window influence on the matching performance:
        * What is the performance of the disambiguation heuristics?
        * How does the time window influence the performance of the heuristics?
        * How does the community size influence the performance of the heuristics?

* [Maispion: A Tool for Analysing and Visualising Open Source Software Developer Communities](https://drive.google.com/open?id=1LCaZpWm5lmOoVKYUcBV-L0BE0VENoRow), 2009

    * Tool for analysing software developer communities.
    * Solves the identity merging issue using the Leveinstein distance.
    * Also includes an extensive study of the temporal commit activity i.e. commit time series.

* [Identity matching and geographical movement of open-source software mailing list participants](https://drive.google.com/open?id=16dQo9asXQbwwBmHssPQJWhkGIMq8BmjC), 2014

    * Large PhD thesis where the identity matching issue has been extensively tackled in section 1.
    * Propose an identity matching algorithm that is able to handle data sets of different orders of magnitude, and is robust to noisy data.
    * Some specs of the algorithm: term-document matrix, edit distance augmentation, tf–idf, singular value decomposition and rank reduction, cosine similarity.
    * Included discussions on optimisation and scalability.


## Papers related to Social Network Analysis (SNA) in open source projects

* [Mining Email Social Networks](https://drive.google.com/open?id=14kZ2UUhBWXFuU1ieoV9Ct75yVvZmBwFc), 2006 from C. Bird and P. Devanbu.

    * Construct social networks of email correspondents.
    * Tackle interesting questions comming right after identity matching, and related to: (1) the social status of different types of OSS participants (2) the relationship of email activity and commit activity (3) the relationship of social status with commit activity.
    * Similar paper: [Mining Email Social Networks in Postgres](https://drive.google.com/file/d/12MZDG-OGUpDUpQrSxfn3aaxDhwCxkqHi/view?usp=sharing)
    * Following paper: [Validity of Network Analyses in Open Source Projects](https://drive.google.com/open?id=1ov16l47xlmb7qG1p9gooriEZr3UwxSpe), 2008, that studies the stability of network metrics like centrality of nodes, in the presence of inadequate and missing data.

* [Latent Social Structure in Open Source Projects](https://drive.google.com/open?id=1Xy89r1WOHOlyWaMNQpYpjNYe6ro1mGfH), 2008 from C. Bird and P. Devanbu.

    * Very well written and verbose paper that talks about the dynamic, self-organizing, latent, and usually not explicitly stated structure under the “bazaar-like” nature of Open Source Software (OSS) Projects.
    * Observes that subcommunities form spontaneously within the developer teams.
    * Gives lessons for how commercial software teams might be organized.
    * Details techniques for detecting community structure in complex networks, extract and study latent subcommunities from the email social network of several projects.
    * Observe also that subcommunities manifest most strongly in technical discussions, and are significantly connected with collaboration behaviour.

* [Applying Social Network Analysis to the Information in CVS Repositories](https://drive.google.com/open?id=1DGwoBPzQRbFKwfOsW1OQjhq9DTM1VR2Z), 2004 from Gregorio Robles, Jesus M. Gonzalez-Barahona.

    * Details the basics concepts of social network analysis.
    * Defines the networks of developers and projects with the corresponding interesting measurements possible.
    * Analysis of the GNOME and Apache networks.

* [Using Social Network Analysis Techniques to Study Collaboration between a FLOSS Community and a Company](https://drive.google.com/open?id=1H9Qlv7tk3Bw2eBPfZ36iX1n9vvYyMLF9), 2008 from Gregorio Robles, Jesus M. Gonzalez-Barahona.

    * Extracts information about the development process of FLOSS projects.
    * Constructs and studies the developers network.
    * Defines and studies network parameters in the context of VCS analysis, like: distance centrality, betweenness centrality, coordination degree, centrality eigenvector etc.
    * Detects the most important events in a development history.
    * Highlights aspects such as efficiency in the development process, release management and leadership turnover.
    * [slides](https://drive.google.com/open?id=1OXkU3NfryQzDSMFAcRprE-kB8t-_505y)
    * Similar paper: [Studying the evolution of libre software projects using publicly available data](https://drive.google.com/open?id=1QbR79jcJPWserBQUg5elYFb5hJ9Dtd8m), 2003.


* [Evolution of the core team of developers in libre software projects](https://drive.google.com/open?id=1idMzTzizw1LM7g53k5I8TyAiw0xONfiD), 2006 from Gregorio Robles, Jesus M. Gonzalez-Barahona.

    * Studies the stability and permanence of the core team in open source projects.
    * Their activity is calculated over time, looking for core team evolution patterns.
    * Evaluation made on the GIMP project that is a case of "code gods".
    * Several visuals in the paper.


## Others

* Paper that looks very interesting but that is written in ... Portuguese: [Quem é quem na lista de discussão? Identificando diferentes e-mails de um mesmo participante](https://drive.google.com/open?id=1O2AqtyrNqRxHG0ldHf9UMSu0cnglH0br)


* Paper that tackled identity matching at MSR 2019, [An Empirical Study of Multiple Names and Email Addresses in OSS Version Control Repositories](https://2019.msrconf.org/details/msr-2019-papers/23/An-Empirical-Study-of-Multiple-Names-and-Email-Addresses-in-OSS-Version-Control-Repositories)
