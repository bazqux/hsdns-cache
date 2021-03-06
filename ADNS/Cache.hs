{-# LANGUAGE RecordWildCards, ViewPatterns, OverloadedStrings,
             TupleSections, PatternGuards #-}
{-|
  Caching asynchronous DNS resolver built on top of
  GNU ADNS <http://www.chiark.greenend.org.uk/~ian/adns/>.

   * Resolves several IP addresses for one host (if available)
     in round-robin fashion.

   * Throttles number of parallel requests (so DNS resolving continues to work
     even under heavy load).

   * Errors are cached too (for one minute).

   * Handles CNAMEs (@hsdns@ returns error for them).

  You should link your program with the /threaded/ runtime-system
  when using this module. In GHC, this is accomplished by specifying @-threaded@
  on the command-line.

  This cache is tested in a long running web-crawler
  (used in <https://bazqux.com>) so it should be safe to use it in real world
  applications.
-}
module ADNS.Cache
    ( -- * Cache
      DnsCache
    , withDnsCache
    , withDnsCacheSettings
    , stopDnsCache

      -- * Settings
    , DnsCacheSettings(..)
    , defaultDnsCacheSettings

      -- * Lookup
    , resolveA, resolveCachedA

      -- * Utils
    , showHostAddress
    ) where

import ADNS hiding (HostName, HostAddress)
import ADNS.Base
import ADNS.Endian
import Data.List
import Data.Word
import qualified Data.HashMap.Strict as HM
import qualified Data.Text as T
import Control.Monad
import Data.Bits
import Control.Concurrent
import qualified Control.Concurrent.MSem as MSem
import Data.Time.Clock.POSIX
import Network.Socket (HostName, HostAddress, inet_ntoa)
--  ^ just to make docs link to Network.Socket instead of ADNS

-- | Asynchronous DNS cache.
data DnsCache
    = DnsCache
      { resolver :: Resolver
      , sem      :: MSem.MSem Int
      , cache    :: MVar (HM.HashMap T.Text (POSIXTime,
                                             Either T.Text (Queue HostAddress)))
      , active :: MVar (HM.HashMap T.Text [MVar (Either String (POSIXTime, [RRAddr]))])
      , settings :: DnsCacheSettings
      , delayMVar :: MVar ()
      }

-- | DNS cache settings.
data DnsCacheSettings
    = DnsCacheSettings
      { dcsMaxParallelRequests :: Int
        -- ^ Throttling of parallel requests. Default: 30
      , dcsRequestDelay :: Maybe Int
        -- ^ Experimental!
        -- Delay in microseconds between subsequent requests to smooth load.
        -- Delay only inserted between real requests to DNS server.
        -- Cached results are returned immediately.
        -- Default: `Nothing`
      }
    deriving (Eq, Show, Read)

defaultDnsCacheSettings = DnsCacheSettings 30 Nothing

data Queue a = Queue ![a] ![a]
    deriving Show

listQ :: [a] -> Queue a
listQ l = Queue l []

rotQ :: Queue a -> Maybe (a, Queue a)
rotQ (Queue [] []) = Nothing
rotQ (Queue (x:xs) ys) = Just (x, Queue xs (x:ys))
rotQ (Queue [] ys) = rotQ (Queue (reverse ys) [])

-- | Create cache and run action passed.
withDnsCache :: (DnsCache -> IO a) -> IO a
withDnsCache = withDnsCacheSettings defaultDnsCacheSettings

-- | Create cache with specified settings and run action passed.
withDnsCacheSettings :: DnsCacheSettings -> (DnsCache -> IO a) -> IO a
withDnsCacheSettings settings act =
    initResolver [NoErrPrint, NoServerWarn, NoSigPipe] $ \ r -> do
        --        ^ there was sigsegv in adns__lprintf when exiting using Ctrl+C
        --          so the warnings printing is suppressed
        c <- newMVar HM.empty
        a <- newMVar HM.empty
        s <- MSem.new $ dcsMaxParallelRequests settings
        d <- newMVar ()
        act (DnsCache r s c a settings d)

-- | Wait till all running resolvers are finished and block further resolvers.
stopDnsCache :: DnsCache -> IO ()
stopDnsCache d =
    replicateM_ (dcsMaxParallelRequests $ settings d) $ MSem.wait (sem d)

-- | Resolve A DNS record.
resolveA :: DnsCache -> HostName -> IO (Either String HostAddress)
resolveA d domain = do
    r <- tryResolveA' d domain
    either id return r

-- | Resolve cached A DNS record.
-- Returns Nothing if host name is not yet cached.
resolveCachedA :: DnsCache -> HostName -> IO (Maybe (Either String HostAddress))
resolveCachedA d domain = do
    r <- tryResolveA' d domain
    return $ either (const Nothing) Just r

type ResultA = Either String HostAddress

-- | Returns action to resolve A or cached resolved action
tryResolveA' :: DnsCache -> HostName -> IO (Either (IO ResultA) ResultA)
tryResolveA' d@(DnsCache {..}) domain
    | any (`elem` ":/?") domain = return $ Right $ Left "Invalid domain name"
    | length domain > 253 = return $ Right $ Left "Domain name too long"
    | any (\ t -> T.length t > 63) (T.split (== '.') $ T.pack domain) =
        return $ Right $ Left "Domain name or component too long"
    | isIPAddr domain = return $ Right $ Right $ ipToWord32 domain
    | otherwise = do
    t <- getPOSIXTime
    mbr <- modifyMVar cache $ \ c ->
        case HM.lookup key c of
             Just (expT, a)
                 | t < expT -> fmap (\(m,r) -> (m, Just r)) $
                               insRot False c expT a
             _ -> return (c, Nothing)
    case mbr of
        Just r -> return $ Right r
        _ -> return $ Left $ do
            res <- modifyMVar active $ \ a ->
                case HM.lookup key a of
                    Just ws -> do
                        w <- newEmptyMVar
                        return (HM.insert key (w:ws) a, takeMVar w)
                    Nothing ->
                        return (HM.insert key [] a, do
--                                    print ("resolve", domain)
                                    r <- MSem.with sem $ do
                                        delay
                                        resolveA' d [] domain
                                    modifyMVar_ active $ \ a ->
                                        case HM.lookup key a of
                                            Just ws -> do
                                                mapM_ (\ w -> putMVar w r) ws
                                                return $ HM.delete key a
                                            Nothing -> return a -- ?
                                    return r)
            ra <- res
            modifyMVar cache $ \ m ->
                case ra of
                    Left e -> insRot True m (t+600) $ Left $ T.pack e
                    Right (max (t+600) -> et, rras) ->
                        insRot True m et $ Right $ listQ [a | RRAddr a <- rras]
    where key = T.pack domain
          err False m _ e = return (m, Left $ T.unpack e)
          err True m t e = return (HM.insert key (t, Left e) m, Left $ T.unpack e)
          insRot f m t (Left e) = err f m t e
          insRot f m t (Right q)
              | Just (a, q') <- rotQ q =
                  return (HM.insert key (t, Right q') m,
                          Right a)
              | otherwise =
                  err f m t "No RRAddr???"
          delay
              | Just dl <- dcsRequestDelay settings =
                  withMVar delayMVar $ const $ threadDelay dl
              | otherwise = return ()

resolveA' :: DnsCache -> [HostName] -> HostName
          -> IO (Either String (POSIXTime, [RRAddr]))
resolveA' d@(DnsCache {..}) parents x
    | length parents > 20 =
        return $ Left $ "Too many CNAMEs " ++ show (x : parents)
    | x `elem` parents =
        return $ Left $ "CNAME loop " ++ show x ++ " already in "
                   ++ show parents
    | otherwise = do
        Answer {..} <- resolver x A
                       [QuoteOk_Query, QuoteOk_AnsHost] >>= takeMVar
        case () of
            _ | status == sOK -> do
--                  print (posixSecondsToUTCTime (realToFrac expires :: POSIXTime))
                  return $ Right (realToFrac expires, [ a | RRA a <- rrs ])
              | status == sPROHIBITEDCNAME
              , Just cn <- cname -> resolveA' d (x:parents) cn
                           -- cycle when CNAME refers another CNAME
            _ -> do
                e <- adnsStrerror status
                s <- adnsErrAbbrev status
                return $ Left $ e ++ " (" ++ s ++ ")"

isIPAddr :: HostName -> Bool
isIPAddr hn = length groups == 4 && all ip groups
    where groups = splitPoints hn
          ip x = length x <= 3 && all (\ e -> e >= '0' && e <= '9') x &&
                 read x <= (255 :: Int)

splitPoints :: HostName -> [String]
splitPoints = filter (/= ".") . groupBy (\ a b -> a /= '.' && b /= '.')

ipToWord32 :: String -> Word32
ipToWord32 ip =
    let [b4,b3,b2,b1] = (map read $ splitPoints ip) :: [Word32]
        mk x1 x2 x3 x4 =
            (x1 `shiftL` 24) + (x2 `shiftL` 16) + (x3 `shiftL` 8) + x4
    in
      case endian of
        BigEndian    -> mk b4 b3 b2 b1
        LittleEndian -> mk b1 b2 b3 b4
        PDPEndian    -> mk b2 b1 b4 b3

-- | Show @HostAddress@ in standard 123.45.67.89 format.
--
-- Unlike 'inet_ntoa' this function is pure and thread-safe.
showHostAddress :: HostAddress -> String
showHostAddress = show . RRAddr


_test :: IO ()
_test = withDnsCacheSettings (DnsCacheSettings 10 (Just 1000000)) $ \ c -> do
    let r hn = do
          h <- resolveA c hn
          putStrLn $ hn ++ ": " ++ either id showHostAddress h
        rn n = replicateM_ n . r
    r "127.0.0.1"
    r "qwerqwer"
    r "qwer.google.com"
    rn 10 "www.huffingtonpost.com"
    rn 10 "twitter.com"
    rn 10 "wordpress.com"
    rn 10 "feedburner.com"
    rn 10 "feeds.feedburner.com"
    print =<< resolveCachedA c "feeds.feedburner.com"
--     replicateM_ 1000 $ do
--         resolveA c "www.blogger.com"
--         resolveA c "google.com"
    s <- MSem.new 0
    replicateM_ 1000 $ do
        forkIO $ void $ resolveA c "blog.bazqux.com" >> MSem.signal s
--        forkIO $ void $ resolveA c "google.com"
    replicateM_ 1000 $ MSem.wait s
    print =<< resolveA c "blog.bazqux.com"
    print =<< resolveA c
          "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.b"
