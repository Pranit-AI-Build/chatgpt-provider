/* ==========================================================================
   ChatGPT Verification Flow Engine
   ========================================================================== */

// Constants & Content Templates
const STREAM_SPEED = 12; // Milliseconds per word for typewriter effect

// Initial document chunks for streaming
const streamChunks = [
  { type: 'element', tag: 'h2', content: 'System Architecture Blueprint: Scale-Ready E-Commerce' },
  { type: 'paragraph', id: 'para-intro', text: 'This document outlines the core architecture of our high-volume retail application. The design emphasizes modularity, separation of concerns, and system reliability under standard peak trading conditions.' },
  { type: 'element', tag: 'h3', content: '1. Compute & Application Layer' },
  { type: 'paragraph', id: 'para-scale', text: 'For our traffic demands, we will deploy a standard application server on a single virtual machine. <span class="risky-assumption" id="assumption-scale">This setup assumes our concurrent users will remain below 500 at peak times</span>, relying on vertical scaling if demand spikes.' },
  { type: 'element', tag: 'h3', content: '2. Data Storage Layer' },
  { type: 'paragraph', id: 'para-db', text: 'Our storage layer will rely on a single primary SQLite database file hosted on the same application server. <span class="risky-assumption" id="assumption-db">This structure is chosen under the assumption that write-locking issues will not impact transactional throughput</span> under sustained write load.' },
  { type: 'element', tag: 'h3', content: '3. Content Delivery & Static Assets' },
  { type: 'paragraph', id: 'para-cdn', text: "To optimize media delivery, assets like product images and stylesheets will be served directly from our application node's local filesystem. We will review global CDN solutions at a future development phase." }
];

// Content for various slider configurations (1: Low/Risky, 2: Mid, 3: High/Enterprise)
const dbConfigs = {
  1: {
    html: 'Our storage layer will rely on a single primary SQLite database file hosted on the same application server. <span class="risky-assumption" id="assumption-db">This structure is chosen under the assumption that write-locking issues will not impact transactional throughput</span> under sustained write load.',
    plain: 'Our storage layer will rely on a single primary SQLite database file hosted on the same application server. This structure is chosen under the assumption that write-locking issues will not impact transactional throughput under sustained write load.',
    label: 'Single SQLite Instance',
    tagClass: 'alert-risky',
    tagText: 'Risky Assumption'
  },
  2: {
    html: 'Our storage layer will rely on a PostgreSQL primary-replica setup with a Redis caching layer to offload reads. <span class="risky-assumption-resolved">This structure is verified to prevent write-locking issues and optimize query throughput</span> under sustained load.',
    plain: 'Our storage layer will rely on a PostgreSQL primary-replica setup with a Redis caching layer to offload reads. This structure is verified to prevent write-locking issues and optimize query throughput under sustained load.',
    diff: 'Our storage layer will rely on <span class="diff-deleted">a single primary SQLite database file hosted on the same application server</span><span class="diff-added">a PostgreSQL primary-replica setup with a Redis caching layer to offload reads</span>. <span class="diff-deleted">This structure is chosen under the assumption that write-locking issues will not impact transactional throughput</span><span class="diff-added">This structure is verified to prevent write-locking issues and optimize query throughput</span> under sustained load.',
    label: 'Postgres + Redis Cache',
    tagClass: 'alert-verified',
    tagText: 'Optimized'
  },
  3: {
    html: 'Our storage layer will rely on a distributed NoSQL database (like Cassandra) with multi-master write replication. <span class="risky-assumption-resolved">This structure guarantees high availability, horizontal write scaling, and sub-millisecond write latencies</span> under heavy write load.',
    plain: 'Our storage layer will rely on a distributed NoSQL database (like Cassandra) with multi-master write replication. This structure guarantees high availability, horizontal write scaling, and sub-millisecond write latencies under heavy write load.',
    diff: 'Our storage layer will rely on <span class="diff-deleted">a single primary SQLite database file hosted on the same application server</span><span class="diff-added">a distributed NoSQL database (like Cassandra) with multi-master write replication</span>. <span class="diff-deleted">This structure is chosen under the assumption that write-locking issues will not impact transactional throughput</span><span class="diff-added">This structure guarantees high availability, horizontal write scaling, and sub-millisecond write latencies</span> under heavy write load.',
    label: 'Distributed NoSQL',
    tagClass: 'alert-verified',
    tagText: 'Verified Scale'
  }
};

const scaleConfigs = {
  1: {
    html: 'For our traffic demands, we will deploy a standard application server on a single virtual machine. <span class="risky-assumption" id="assumption-scale">This setup assumes our concurrent users will remain below 500 at peak times</span>, relying on vertical scaling if demand spikes.',
    plain: 'For our traffic demands, we will deploy a standard application server on a single virtual machine. This setup assumes our concurrent users will remain below 500 at peak times, relying on vertical scaling if demand spikes.',
    label: '500 peak users',
    tagClass: 'alert-risky',
    tagText: 'Risky Assumption'
  },
  2: {
    html: 'For our traffic demands, we will deploy a containerized application server behind a load balancer with 2 active instances. <span class="risky-assumption-resolved">This setup is verified to handle up to 5,000 concurrent users at peak times</span>, utilizing horizontal scale rules.',
    plain: 'For our traffic demands, we will deploy a containerized application server behind a load balancer with 2 active instances. This setup is verified to handle up to 5,000 concurrent users at peak times, utilizing horizontal scale rules.',
    diff: 'For our traffic demands, we will deploy <span class="diff-deleted">a standard application server on a single virtual machine</span><span class="diff-added">a containerized application server behind a load balancer with 2 active instances</span>. <span class="diff-deleted">This setup assumes our concurrent users will remain below 500 at peak times</span><span class="diff-added">This setup is verified to handle up to 5,000 concurrent users at peak times</span>, utilizing horizontal scale rules.',
    label: '5k peak users',
    tagClass: 'alert-verified',
    tagText: 'Mid-Scale'
  },
  3: {
    html: 'For our traffic demands, we will deploy a multi-region auto-scaling Kubernetes cluster with global load balancers. <span class="risky-assumption-resolved">This setup dynamically scales to support over 100,000 concurrent users at peak times</span>, using multi-zone replication.',
    plain: 'For our traffic demands, we will deploy a multi-region auto-scaling Kubernetes cluster with global load balancers. This setup dynamically scales to support over 100,000 concurrent users at peak times, using multi-zone replication.',
    diff: 'For our traffic demands, we will deploy <span class="diff-deleted">a standard application server on a single virtual machine</span><span class="diff-added">a multi-region auto-scaling Kubernetes cluster with global load balancers</span>. <span class="diff-deleted">This setup assumes our concurrent users will remain below 500 at peak times</span><span class="diff-added">This setup dynamically scales to support over 100,000 concurrent users at peak times</span>, using multi-zone replication.',
    label: '100k+ peak users',
    tagClass: 'alert-verified',
    tagText: 'Verified Scale'
  }
};

// Global State
let currentState = 0; // 0: Initial, 1: Streaming, 2: Checking, 3: Changing, 4: Seeing, 5: Finished
let streamTimer = null;
let changeTimers = {};
let sliderStates = {
  scale: 1,
  db: 1
};
let hasChanged = false;

// Elements
const landingContainer = document.getElementById('landingContainer');
const conversationContainer = document.getElementById('conversationContainer');
const documentCanvas = document.getElementById('documentCanvas');
const verificationPanel = document.getElementById('verificationPanel');
const verificationSkeleton = document.getElementById('verificationSkeleton');
const verificationActiveContent = document.getElementById('verificationActiveContent');
const manifestoPanel = document.getElementById('manifestoPanel');
const chatInput = document.getElementById('chatInput');
const sendBtn = document.getElementById('sendBtn');
const docStatusBadge = document.getElementById('docStatusBadge');
const currentStateText = document.getElementById('currentStateText');

// Initial Setup
window.addEventListener('DOMContentLoaded', () => {
  setManualState(0);
});

// Trigger the Flow (auto-simulation from clicking Send)
function triggerFlow() {
  if (currentState !== 0) return;
  
  // Transition from Initial to Streaming
  goToState1();
}

// Reset entire flow
function resetFlow() {
  setManualState(0);
}

// State transition logic
function goToState0() {
  currentState = 0;
  updateUIForState();
  
  // Reset fields
  chatInput.value = "Create an architecture blueprint for a scaling e-commerce backend.";
  chatInput.disabled = false;
  sendBtn.disabled = false;
  documentCanvas.innerHTML = "";
  
  // Reset Sliders
  document.getElementById('sliderScale').value = 1;
  document.getElementById('sliderDb').value = 1;
  sliderStates.scale = 1;
  sliderStates.db = 1;
  hasChanged = false;
  
  updateTicks('scale', 1);
  updateTicks('db', 1);
  
  // Hide panel
  verificationPanel.classList.remove('panel-open');
}

function goToState1() {
  currentState = 1;
  updateUIForState();
  
  // Start Streaming Text
  streamTextContent(() => {
    // Automatically transition to State 2: Checking
    goToState2();
  });
}

function goToState2() {
  currentState = 2;
  updateUIForState();
  
  // Set slider inputs
  document.getElementById('sliderScale').value = 1;
  document.getElementById('sliderDb').value = 1;
  sliderStates.scale = 1;
  sliderStates.db = 1;
  updateTicks('scale', 1);
  updateTicks('db', 1);
}

function goToState3(sliderId, value) {
  currentState = 3;
  updateUIForState();
  
  // Trigger highlight pulsing and dimming on target paragraph
  const targetPara = document.getElementById(`para-${sliderId}`);
  const allParas = documentCanvas.querySelectorAll('p');
  
  // Dim others, pulse target
  allParas.forEach(p => {
    if (p.id === `para-${sliderId}`) {
      p.classList.add('para-pulsing');
      p.classList.remove('para-dimmed');
    } else {
      p.classList.add('para-dimmed');
      p.classList.remove('para-pulsing');
    }
  });

  // Highlight assumption tag inside target paragraph
  const assumptionTag = targetPara.querySelector('.risky-assumption');
  if (assumptionTag) {
    assumptionTag.classList.add('pulse-active');
  }

  // Set card active visual
  document.getElementById(`card-${sliderId}`).classList.add('active-focus');

  // Trigger State 4 (Seeing) automatically after 1.5s
  clearTimeout(changeTimers[sliderId]);
  changeTimers[sliderId] = setTimeout(() => {
    goToState4(sliderId, value);
  }, 1500);
}

function goToState4(sliderId, value) {
  currentState = 4;
  updateUIForState();
  
  // Apply the text swap with Diff formatting
  const config = sliderId === 'scale' ? scaleConfigs[value] : dbConfigs[value];
  const targetPara = document.getElementById(`para-${sliderId}`);
  
  // Insert diff content
  if (config.diff) {
    targetPara.innerHTML = config.diff;
  } else {
    // Fallback to plain html if no diff (e.g. back to pos 1)
    targetPara.innerHTML = config.html;
  }
  
  // Highlight card changes
  const card = document.getElementById(`card-${sliderId}`);
  card.classList.remove('active-focus');
  
  const tag = document.getElementById(`tag-${sliderId}`);
  tag.className = `assumption-tag ${config.tagClass}`;
  tag.textContent = config.tagText;
  
  const desc = document.getElementById(`val-desc-${sliderId}`);
  desc.innerHTML = config.label;

  // Wait 2.0s, then fade highlights to neutral and check if both are verified
  clearTimeout(changeTimers[sliderId + '-fade']);
  changeTimers[sliderId + '-fade'] = setTimeout(() => {
    // Clear diff formatting (remove red text, keep green additions but blend them into neutral)
    targetPara.innerHTML = config.html;
    
    // Clear dimming on other paragraphs
    const allParas = documentCanvas.querySelectorAll('p');
    allParas.forEach(p => {
      p.classList.remove('para-dimmed');
      p.classList.remove('para-pulsing');
    });
    
    // Check if both sliders are in verified positions (3) or at least upgraded
    if (sliderStates.scale === 3 && sliderStates.db === 3) {
      goToState5();
    } else {
      currentState = 2; // return to checking state (interactive sandbox)
      updateUIForState();
    }
  }, 2000);
}

function goToState5() {
  currentState = 5;
  updateUIForState();
  
  // Force both sliders to Enterprise (3) if jumping manually
  document.getElementById('sliderScale').value = 3;
  document.getElementById('sliderDb').value = 3;
  sliderStates.scale = 3;
  sliderStates.db = 3;
  updateTicks('scale', 3);
  updateTicks('db', 3);
  
  // Update left canvas text to be fully read-only and clean
  document.getElementById('para-scale').innerHTML = scaleConfigs[3].plain;
  document.getElementById('para-db').innerHTML = dbConfigs[3].plain;
  
  // Remove all highlighting classes
  const allParas = documentCanvas.querySelectorAll('p');
  allParas.forEach(p => {
    p.className = "";
  });
  
  // Update card status labels
  document.getElementById('tag-scale').className = 'assumption-tag alert-verified';
  document.getElementById('tag-scale').textContent = 'Verified';
  document.getElementById('val-desc-scale').textContent = scaleConfigs[3].label;

  document.getElementById('tag-db').className = 'assumption-tag alert-verified';
  document.getElementById('tag-db').textContent = 'Verified';
  document.getElementById('val-desc-db').textContent = dbConfigs[3].label;
}

// Master UI State updater
function updateUIForState() {
  // Clear any active intervals/timers if we are leaving a state
  if (currentState !== 1) {
    clearInterval(streamTimer);
  }
  
  // Update state labels
  const stateLabels = ['Initial', 'Loading & Streaming', 'Checking Assumptions', 'Changing Specification', 'Seeing Diff', 'Finished'];
  if (currentStateText) {
    currentStateText.textContent = stateLabels[currentState];
  }
  
  // Update inspector button states
  document.querySelectorAll('.state-btn').forEach((btn, idx) => {
    if (idx === currentState) {
      btn.classList.add('active');
    } else {
      btn.classList.remove('active');
    }
  });

  // State-specific layout adjustments
  switch (currentState) {
    case 0: // Initial
      landingContainer.style.display = 'flex';
      conversationContainer.style.display = 'none';
      chatInput.disabled = false;
      sendBtn.disabled = false;
      docStatusBadge.textContent = 'DRAFT';
      verificationPanel.classList.remove('panel-open');
      break;
      
    case 1: // Streaming
      landingContainer.style.display = 'none';
      conversationContainer.style.display = 'block';
      chatInput.disabled = true;
      sendBtn.disabled = true;
      docStatusBadge.textContent = 'STREAMING...';
      
      // Open panel, show skeletons
      verificationPanel.classList.add('panel-open');
      verificationSkeleton.style.display = 'block';
      verificationActiveContent.style.display = 'none';
      manifestoPanel.style.display = 'none';
      break;
      
    case 2: // Checking
      landingContainer.style.display = 'none';
      conversationContainer.style.display = 'block';
      chatInput.disabled = true;
      sendBtn.disabled = true;
      docStatusBadge.textContent = 'NEEDS VERIFICATION';
      
      // Slide open panel, reveal controls, hide skeleton
      verificationPanel.classList.add('panel-open');
      verificationSkeleton.style.display = 'none';
      verificationActiveContent.style.display = 'block';
      if (hasChanged) {
        manifestoPanel.style.display = 'block';
        document.getElementById('copyWorkBtn').classList.add('glowing-active');
      } else {
        manifestoPanel.style.display = 'none';
        document.getElementById('copyWorkBtn').classList.remove('glowing-active');
      }
      break;
      
    case 3: // Changing
      docStatusBadge.textContent = 'VERIFYING...';
      break;
      
    case 4: // Seeing
      docStatusBadge.textContent = 'SWAPPING CONTENT';
      break;
      
    case 5: // Finished
      landingContainer.style.display = 'none';
      conversationContainer.style.display = 'block';
      chatInput.disabled = true;
      sendBtn.disabled = true;
      docStatusBadge.textContent = 'VERIFIED & COMPLETE';
      
      // Show Manifesto and glow the Copy button
      verificationPanel.classList.add('panel-open');
      verificationSkeleton.style.display = 'none';
      verificationActiveContent.style.display = 'block';
      manifestoPanel.style.display = 'block';
      
      document.getElementById('copyWorkBtn').classList.add('glowing-active');
      break;
  }
}

// Inspector Button controls (Manual overrides)
function setManualState(stateNum) {
  // Clear any running timers
  clearInterval(streamTimer);
  Object.values(changeTimers).forEach(clearTimeout);
  changeTimers = {};

  if (stateNum === 0) {
    goToState0();
  } else if (stateNum === 1) {
    // Jump straight to streaming layout, populate some initial text instantly
    goToState1();
  } else if (stateNum === 2) {
    // Instantly complete streaming text
    populateFullInitialText();
    goToState2();
  } else if (stateNum === 3) {
    // Populate text and trigger load scale change
    populateFullInitialText();
    goToState2();
    // Simulate user sliding to Enterprise (3)
    document.getElementById('sliderScale').value = 3;
    sliderStates.scale = 3;
    updateTicks('scale', 3);
    goToState3('scale', 3);
  } else if (stateNum === 4) {
    // Populate text and show diff directly
    populateFullInitialText();
    goToState2();
    document.getElementById('sliderScale').value = 3;
    sliderStates.scale = 3;
    updateTicks('scale', 3);
    goToState4('scale', 3);
  } else if (stateNum === 5) {
    populateFullInitialText();
    goToState5();
  }
}

// Populates left canvas immediately (helper for manual state jumps)
function populateFullInitialText() {
  documentCanvas.innerHTML = "";
  streamChunks.forEach(chunk => {
    if (chunk.type === 'element') {
      const el = document.createElement(chunk.tag);
      el.textContent = chunk.content;
      documentCanvas.appendChild(el);
    } else {
      const p = document.createElement('p');
      p.id = chunk.id;
      p.innerHTML = chunk.text;
      documentCanvas.appendChild(p);
    }
  });
}

// Text streaming animation engine (ChatGPT-style)
function streamTextContent(onComplete) {
  documentCanvas.innerHTML = "";
  let chunkIndex = 0;
  let wordIndex = 0;
  let currentWords = [];
  let currentElement = null;

  function processNextChunk() {
    if (chunkIndex >= streamChunks.length) {
      if (onComplete) onComplete();
      return;
    }

    const chunk = streamChunks[chunkIndex];

    if (chunk.type === 'element') {
      // Direct insertion of headers
      currentElement = document.createElement(chunk.tag);
      currentElement.textContent = chunk.content;
      documentCanvas.appendChild(currentElement);
      chunkIndex++;
      setTimeout(processNextChunk, 200);
    } else {
      // Typewriter streaming of paragraphs
      currentElement = document.createElement('p');
      currentElement.id = chunk.id;
      documentCanvas.appendChild(currentElement);
      
      // Parse plain content into words
      // Since some paragraphs have risky assumption markup, we'll stream standard innerHTML blocks
      // To simulate it nicely without breaking HTML tags, we split by spaces but preserve elements
      if (chunk.text.includes('<span')) {
        // Stream the paragraph. To keep it simple and clean, we render the pre-span text first,
        // then render the assumption span, then the post-span text.
        // Let's split by HTML tags
        const parts = chunk.text.split(/(<span.*?<\/span>)/g);
        let partIndex = 0;
        
        function streamHtmlPart() {
          if (partIndex >= parts.length) {
            chunkIndex++;
            setTimeout(processNextChunk, 200);
            return;
          }
          
          const part = parts[partIndex];
          if (part.startsWith('<span')) {
            // Stream the assumption block rapidly
            currentElement.innerHTML += part;
            partIndex++;
            // Scroll to bottom of canvas
            documentCanvas.scrollTop = documentCanvas.scrollHeight;
            setTimeout(streamHtmlPart, 300);
          } else {
            // Type normal text word-by-word
            const words = part.split(' ').filter(w => w.length > 0);
            let wIdx = 0;
            
            function typeWord() {
              if (wIdx >= words.length) {
                partIndex++;
                setTimeout(streamHtmlPart, 100);
                return;
              }
              currentElement.innerHTML += (currentElement.innerHTML === "" ? "" : " ") + words[wIdx];
              wIdx++;
              // Scroll to bottom of canvas
              documentCanvas.scrollTop = documentCanvas.scrollHeight;
              setTimeout(typeWord, STREAM_SPEED);
            }
            typeWord();
          }
        }
        streamHtmlPart();
      } else {
        const words = chunk.text.split(' ');
        let wIdx = 0;
        
        function typePlainWord() {
          if (wIdx >= words.length) {
            chunkIndex++;
            setTimeout(processNextChunk, 200);
            return;
          }
          currentElement.innerHTML += (currentElement.innerHTML === "" ? "" : " ") + words[wIdx];
          wIdx++;
          // Scroll to bottom of canvas
          documentCanvas.scrollTop = documentCanvas.scrollHeight;
          setTimeout(typePlainWord, STREAM_SPEED);
        }
        typePlainWord();
      }
    }
  }

  processNextChunk();
}

// Slider Input Handlers
function handleSliderInput(sliderId, value) {
  const val = parseInt(value);
  sliderStates[sliderId] = val;
  hasChanged = true;
  updateTicks(sliderId, val);
  
  // Jump to State 3 (Changing)
  goToState3(sliderId, val);
}

// Tick highlights helper
function updateTicks(sliderId, activeValue) {
  const sliderContainer = document.getElementById(`sliderScale`).parentNode;
  const slider = sliderId === 'scale' ? document.getElementById('sliderScale') : document.getElementById('sliderDb');
  const ticks = slider.parentNode.querySelectorAll('.tick');
  
  ticks.forEach((tick, index) => {
    if (index + 1 === activeValue) {
      tick.classList.add('active');
    } else {
      tick.classList.remove('active');
    }
  });

  // Update manifesto preview tags live
  if (sliderId === 'scale') {
    const scaleText = document.getElementById('manifesto-scale-text');
    scaleText.textContent = activeValue === 1 ? 'Small Biz Scale (500 users)' : activeValue === 2 ? 'Mid-Market Scale (5k users)' : 'Enterprise Scale (100k+ users)';
  } else if (sliderId === 'db') {
    const dbText = document.getElementById('manifesto-db-text');
    dbText.textContent = activeValue === 1 ? 'Single Relational Database' : activeValue === 2 ? 'Optimized SQL Primary-Replica' : 'Distributed NoSQL Database Storage';
  }
}

// Tick click support
function setSliderValue(sliderId, value) {
  const slider = sliderId === 'scale' ? document.getElementById('sliderScale') : document.getElementById('sliderDb');
  slider.value = value;
  handleSliderInput(sliderId, value);
}

// Clipboard copying utility
function copyBlueprintToClipboard() {
  if (currentState < 2) return;
  
  // Format clean markdown text to copy
  const cleanContent = `## System Architecture Blueprint: Scale-Ready E-Commerce

This document outlines the core architecture of our high-volume retail application. The design emphasizes modularity, separation of concerns, and system reliability under standard peak trading conditions.

### 1. Compute & Application Layer
${scaleConfigs[sliderStates.scale].plain}

### 2. Data Storage Layer
${dbConfigs[sliderStates.db].plain}

### 3. Content Delivery & Static Assets
To optimize media delivery, assets like product images and stylesheets will be served directly from our application node's local filesystem. We will review global CDN solutions at a future development phase.

---
VERIFIED SPECIFICATION MANIFESTO
- Load Scale Capability: Verified for ${scaleConfigs[sliderStates.scale].label}
- Database Architecture: Verified for ${dbConfigs[sliderStates.db].label}
- Fault Tolerance: Zero single point of failures identified.`;

  navigator.clipboard.writeText(cleanContent).then(() => {
    showToast();
    
    // Animate copy button text briefly
    const copyText = document.getElementById('copyBtnText');
    const oldText = copyText.textContent;
    copyText.textContent = "Copied!";
    setTimeout(() => {
      copyText.textContent = oldText;
    }, 2000);
  }).catch(err => {
    console.error('Failed to copy text: ', err);
  });
}

// Toast indicator helper
function showToast() {
  const toast = document.getElementById('toast');
  toast.classList.add('show');
  setTimeout(() => {
    toast.classList.remove('show');
  }, 2500);
}
