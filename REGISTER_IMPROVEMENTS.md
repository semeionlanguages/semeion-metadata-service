# Register Pipeline Improvements

## Problem
The register script was shutting down after processing only 3 batches (~1,500 entries) out of 34,000+ missing entries.

## Root Causes Identified
1. **No error handling** - Any API error would crash the entire pipeline
2. **No retry logic** - Transient network/API failures would stop processing
3. **Rate limiting issues** - OpenAI API rate limits could cause failures
4. **No recovery mechanism** - Failed batches would stop the entire process

## Solutions Implemented

### 1. Robust Retry Logic with Exponential Backoff
- Added `max_retries=5` to `classify_formality()` function
- Implements exponential backoff: waits 2^attempt + 0.1*attempt seconds between retries
- Gracefully handles transient API errors without crashing

### 2. Error Handling & Logging
- Catches all exceptions during API calls
- Logs detailed error messages for debugging
- Returns `None` on failure instead of crashing
- Tracks failed entries for later retry

### 3. Rate Limiting Protection
- Added `timeout=30.0` to OpenAI API calls
- Increased sleep time from `0` to `0.05` seconds between entries
- Prevents hitting API rate limits

### 4. Batch-Level Error Recovery
- Background task now catches batch-level errors
- Allows up to 3 consecutive batch failures before stopping
- Waits 5 seconds before retrying failed batches
- Continues processing even if individual entries fail

### 5. Progress Tracking
- Tracks both successful and failed entries
- Reports failure count in logs
- Stores failed entries for potential retry later

## Expected Behavior Now

✅ **Continues processing** even when individual API calls fail  
✅ **Retries automatically** with exponential backoff  
✅ **Handles rate limits** gracefully  
✅ **Recovers from batch errors** up to 3 consecutive failures  
✅ **Completes all 34,000+ entries** without manual intervention  
✅ **Logs detailed progress** including failure counts  

## Usage

The pipeline will now run continuously until:
- All entries are processed successfully, OR
- 3 consecutive batch failures occur (indicating a serious issue)

Failed individual entries are logged but don't stop the pipeline. They can be retried later if needed.

## Monitoring

Watch for these log messages:
- `[RETRY]` - Individual entry retry attempts
- `[SKIP]` - Entry failed after all retries (will continue with next)
- `[BACKGROUND] Batch X complete. Total: Y (Failed in batch: Z)` - Batch completion with failure count
- `[BACKGROUND] All entries processed!` - Pipeline completed successfully
