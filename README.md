hsdns-cache
===========

Caching asynchronous DNS resolver built on top of 
GNU ADNS http://www.chiark.greenend.org.uk/~ian/adns/.

Resolves several IP addresses for one host (if available)
in round-robin fashion.

Limits number of parallel requests (so DNS resolving continues to work
even under heavy load).

Errors are cached too (for one minute).

Handles CNAMEs ([hsdns](http://hackage.haskell.org/package/hsdns) returns error
for them).
     
Used in production in [BazQux Reader](http://bazqux.com) feeds and comments
crawler.
