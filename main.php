<?php

class HeapItem {
    public $key;
    public $expiration;
    public $index;

    public function __construct($key, $expiration) {
        $this->key = $key;
        $this->expiration = $expiration;
        $this->index = 0;
    }
}

class MinHeap {
    private $heap = [];
    private $size = 0;

    public function count() {
        return $this->size;
    }

    public function peek() {
        if ($this->size === 0) {
            return null;
        }
        return $this->heap[0];
    }

    public function push($item) {
        $item->index = $this->size;
        $this->heap[$this->size] = $item;
        $this->siftUp($this->size);
        $this->size++;
    }

    public function pop() {
        if ($this->size === 0) {
            return null;
        }
        
        $result = $this->heap[0];
        $this->size--;
        
        if ($this->size > 0) {
            $this->heap[0] = $this->heap[$this->size];
            $this->heap[0]->index = 0;
            $this->siftDown(0);
        }
        
        unset($this->heap[$this->size]);
        $result->index = -1;
        return $result;
    }

    public function removeAt($index) {
        if ($index < 0 || $index >= $this->size) {
            return null;
        }
        
        $removed = $this->heap[$index];
        
        $lastIndex = $this->size - 1;
        if ($index === $lastIndex) {
            $this->size--;
            unset($this->heap[$this->size]);
            $removed->index = -1;
            return $removed;
        }
        
        $this->swap($index, $lastIndex);
        $this->size--;
        unset($this->heap[$this->size]);
        $removed->index = -1;
        
        $this->fix($index);
        
        return $removed;
    }

    public function fix($index) {
        if ($index >= $this->size) {
            return;
        }
        
        $parentIndex = intdiv($index - 1, 2);
        if ($index > 0 && $this->heap[$index]->expiration < $this->heap[$parentIndex]->expiration) {
            $this->siftUp($index);
        } else {
            $this->siftDown($index);
        }
    }

    public function clear() {
        $this->heap = [];
        $this->size = 0;
    }

    private function siftUp($index) {
        while ($index > 0) {
            $parentIndex = intdiv($index - 1, 2);
            if ($this->heap[$index]->expiration >= $this->heap[$parentIndex]->expiration) {
                break;
            }
            
            $this->swap($index, $parentIndex);
            $index = $parentIndex;
        }
    }

    private function siftDown($index) {
        while ($index < $this->size) {
            $leftChild = 2 * $index + 1;
            $rightChild = 2 * $index + 2;
            $smallest = $index;
            
            if ($leftChild < $this->size && $this->heap[$leftChild]->expiration < $this->heap[$smallest]->expiration) {
                $smallest = $leftChild;
            }
            
            if ($rightChild < $this->size && $this->heap[$rightChild]->expiration < $this->heap[$smallest]->expiration) {
                $smallest = $rightChild;
            }
            
            if ($smallest === $index) {
                break;
            }
            
            $this->swap($index, $smallest);
            $index = $smallest;
        }
    }

    private function swap($i, $j) {
        $temp = $this->heap[$i];
        $this->heap[$i] = $this->heap[$j];
        $this->heap[$j] = $temp;
        
        $this->heap[$i]->index = $i;
        $this->heap[$j]->index = $j;
    }
}

class CacheItem {
    public $value;
    public $heapItem;
    public $created;
    public $accessTime;

    public function __construct($value, $heapItem) {
        $this->value = $value;
        $this->heapItem = $heapItem;
        $this->created = microtime(true);
        $this->accessTime = microtime(true);
    }
}

class CacheStats {
    public $hits = 0;
    public $misses = 0;
    public $evictions = 0;
    public $expirations = 0;
}

class OptimizedTTLCache {
    private $capacity;
    private $defaultTTL;
    private $items = [];
    private $expiryHeap;
    private $stats;
    private $cleanupInterval = 1;
    private $lastCleanupTime = 0;
    private $running = true;
    private $lockFile;
    private $useLocking;

    public function __construct($capacity = 1000, $ttl = 300, $useLocking = false) {
        if ($capacity <= 0) {
            $capacity = 1000;
        }
        if ($ttl <= 0) {
            $ttl = 300;
        }
        
        $this->capacity = $capacity;
        $this->defaultTTL = $ttl;
        $this->expiryHeap = new MinHeap();
        $this->stats = new CacheStats();
        $this->useLocking = $useLocking;
        
        if ($this->useLocking) {
            $this->lockFile = sys_get_temp_dir() . '/ttl_cache.lock';
        }
        
        register_shutdown_function([$this, 'stop']);
    }

    private function lock() {
        if (!$this->useLocking) {
            return null;
        }
        
        $lock = fopen($this->lockFile, 'w');
        if ($lock) {
            flock($lock, LOCK_EX);
        }
        return $lock;
    }

    private function unlock($lock) {
        if (!$this->useLocking || !$lock) {
            return;
        }
        
        flock($lock, LOCK_UN);
        fclose($lock);
    }

    public function set($key, $value) {
        $this->setWithTTL($key, $value, $this->defaultTTL);
    }

    public function setWithTTL($key, $value, $ttl) {
        if ($ttl <= 0) {
            $ttl = $this->defaultTTL;
        }

        $lock = $this->lock();
        
        $now = microtime(true);
        $expiration = $now + $ttl;
        
        if (isset($this->items[$key])) {
            $existing = $this->items[$key];
            $existing->value = $value;
            $existing->accessTime = $now;
            $existing->heapItem->expiration = $expiration;
            $this->expiryHeap->fix($existing->heapItem->index);
            $this->unlock($lock);
            return;
        }
        
        if (count($this->items) >= $this->capacity) {
            $this->evictOne();
        }
        
        $heapItem = new HeapItem($key, $expiration);
        $item = new CacheItem($value, $heapItem);
        
        $this->items[$key] = $item;
        $this->expiryHeap->push($heapItem);
        
        $this->performCleanupIfNeeded();
        $this->unlock($lock);
    }

    private function evictOne() {
        if ($this->expiryHeap->count() === 0) {
            return;
        }
        
        $heapItem = $this->expiryHeap->pop();
        if ($heapItem && isset($this->items[$heapItem->key])) {
            unset($this->items[$heapItem->key]);
            $this->stats->evictions++;
        }
    }

    public function get($key) {
        $lock = $this->lock();
        
        if (!isset($this->items[$key])) {
            $this->stats->misses++;
            $this->unlock($lock);
            return [null, false];
        }
        
        $item = $this->items[$key];
        $now = microtime(true);
        
        if ($now > $item->heapItem->expiration) {
            $this->expiryHeap->removeAt($item->heapItem->index);
            unset($this->items[$key]);
            $this->stats->expirations++;
            $this->stats->misses++;
            $this->unlock($lock);
            return [null, false];
        }
        
        $item->accessTime = $now;
        $this->stats->hits++;
        
        $this->performCleanupIfNeeded();
        $this->unlock($lock);
        return [$item->value, true];
    }

    private function performCleanupIfNeeded() {
        $now = microtime(true);
        if ($now - $this->lastCleanupTime >= $this->cleanupInterval) {
            $this->cleanupExpired();
            $this->lastCleanupTime = $now;
        }
    }

    public function cleanupExpired() {
        $count = 0;
        $now = microtime(true);
        
        while ($this->expiryHeap->count() > 0) {
            $heapItem = $this->expiryHeap->peek();
            if (!$heapItem) {
                break;
            }
            
            if ($now <= $heapItem->expiration) {
                break;
            }
            
            $heapItem = $this->expiryHeap->pop();
            if (isset($this->items[$heapItem->key])) {
                unset($this->items[$heapItem->key]);
                $count++;
            }
        }
        
        $this->stats->expirations += $count;
        return $count;
    }

    public function size() {
        $lock = $this->lock();
        $size = count($this->items);
        $this->unlock($lock);
        return $size;
    }

    public function stop() {
        if ($this->running) {
            $lock = $this->lock();
            $this->running = false;
            $this->cleanupExpired();
            $this->unlock($lock);
        }
    }

    public function delete($key) {
        $lock = $this->lock();
        
        if (!isset($this->items[$key])) {
            $this->unlock($lock);
            return false;
        }
        
        $item = $this->items[$key];
        $this->expiryHeap->removeAt($item->heapItem->index);
        unset($this->items[$key]);
        
        $this->unlock($lock);
        return true;
    }

    public function clear() {
        $lock = $this->lock();
        $this->items = [];
        $this->expiryHeap->clear();
        $this->stats = new CacheStats();
        $this->unlock($lock);
    }

    public function getStats() {
        $lock = $this->lock();
        $stats = clone $this->stats;
        $this->unlock($lock);
        return $stats;
    }

    public function keys() {
        $lock = $this->lock();
        
        $keys = [];
        $now = microtime(true);
        
        foreach ($this->items as $key => $item) {
            if ($now <= $item->heapItem->expiration) {
                $keys[] = $key;
            } else {
                $this->expiryHeap->removeAt($item->heapItem->index);
                unset($this->items[$key]);
            }
        }
        
        $this->unlock($lock);
        return $keys;
    }

    public function contains($key) {
        $lock = $this->lock();
        
        if (!isset($this->items[$key])) {
            $this->unlock($lock);
            return false;
        }
        
        $item = $this->items[$key];
        $valid = microtime(true) <= $item->heapItem->expiration;
        
        if (!$valid) {
            $this->expiryHeap->removeAt($item->heapItem->index);
            unset($this->items[$key]);
        }
        
        $this->unlock($lock);
        return $valid;
    }

    public function getWithExpiry($key) {
        $lock = $this->lock();
        
        if (!isset($this->items[$key])) {
            $this->stats->misses++;
            $this->unlock($lock);
            return [null, 0, false];
        }
        
        $item = $this->items[$key];
        $now = microtime(true);
        
        if ($now > $item->heapItem->expiration) {
            $this->expiryHeap->removeAt($item->heapItem->index);
            unset($this->items[$key]);
            $this->stats->misses++;
            $this->stats->expirations++;
            $this->unlock($lock);
            return [null, 0, false];
        }
        
        $this->stats->hits++;
        $value = $item->value;
        $expiration = $item->heapItem->expiration;
        
        $this->unlock($lock);
        return [$value, $expiration, true];
    }

    public function peek($key) {
        $lock = $this->lock();
        
        if (!isset($this->items[$key])) {
            $this->unlock($lock);
            return [null, false];
        }
        
        $item = $this->items[$key];
        $valid = microtime(true) <= $item->heapItem->expiration;
        $value = $valid ? $item->value : null;
        
        $this->unlock($lock);
        return [$value, $valid];
    }

    public function resize($newCapacity) {
        $lock = $this->lock();
        
        if ($newCapacity <= 0) {
            $newCapacity = 1000;
        }
        
        if ($newCapacity < $this->capacity) {
            while (count($this->items) > $newCapacity) {
                $this->evictOne();
            }
        }
        
        $this->capacity = $newCapacity;
        $this->unlock($lock);
    }

    public function verifyHeap() {
        $lock = $this->lock();
        
        $valid = true;
        $heapSize = $this->expiryHeap->count();
        
        for ($i = 0; $i < $heapSize; $i++) {
            $item = $this->expiryHeap->peek();
            if (!$item) {
                $valid = false;
                break;
            }
            
            if ($item->index !== $i) {
                $valid = false;
                break;
            }
            
            if (!isset($this->items[$item->key])) {
                $valid = false;
                break;
            }
        }
        
        $this->unlock($lock);
        return $valid;
    }

    public function capacity() {
        return $this->capacity;
    }

    public function __destruct() {
        $this->stop();
    }
}

function demo() {
    echo "=== Optimized TTL Cache with Heap ===\n\n";
    
    $cache = new OptimizedTTLCache(5, 2, false);
    
    echo "Initial capacity: " . $cache->capacity() . "\n";
    
    $cache->setWithTTL("fast", "expires in 1s", 1);
    $cache->setWithTTL("medium", "expires in 3s", 3);
    $cache->setWithTTL("slow", "expires in 5s", 5);
    
    echo "Initial size: " . $cache->size() . "\n";
    
    sleep(2);
    
    $items = ["fast", "medium", "slow"];
    foreach ($items as $key) {
        list($val, $found) = $cache->get($key);
        if ($found) {
            echo "$key: $val\n";
        } else {
            echo "$key: expired\n";
        }
    }
    
    echo "\nAdding 10 items (capacity is 5)...\n";
    for ($i = 0; $i < 10; $i++) {
        $key = "item$i";
        $cache->setWithTTL($key, $i, $i + 1);
    }
    
    echo "Size after adding: " . $cache->size() . "\n";
    
    $cache->resize(8);
    echo "Capacity after resize: " . $cache->capacity() . "\n";
    
    $heapValid = $cache->verifyHeap();
    echo "Heap integrity check: " . ($heapValid ? 'true' : 'false') . "\n";
    
    sleep(6);
    
    echo "Size after cleanup: " . $cache->size() . "\n";
    
    $stats = $cache->getStats();
    echo "Stats - Hits: " . $stats->hits . 
         ", Misses: " . $stats->misses . 
         ", Evictions: " . $stats->evictions . 
         ", Expirations: " . $stats->expirations . "\n";
    
    $keys = $cache->keys();
    echo "Active keys: " . implode(', ', $keys) . "\n";
    
    echo "\n=== Demo Complete ===\n";
}

if (php_sapi_name() === 'cli') {
    demo();
}
