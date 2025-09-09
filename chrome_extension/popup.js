let selectedFile = null;
let conversationId = null;
let waitingMessageElement = null;

document.addEventListener('DOMContentLoaded', function() {
    const chatContainer = document.getElementById('chat-container');
    const messageInput = document.getElementById('message-input');
    const sendButton = document.getElementById('send-btn');
    const uploadButton = document.getElementById('upload-btn');
    const clearButton = document.getElementById('clear-btn');
    const fileInput = document.getElementById('file-input');
    const fileNameDisplay = document.getElementById('file-name');

    // Function to show waiting message
    function showWaitingMessage() {
        // Remove any existing waiting message
        if (waitingMessageElement) {
            waitingMessageElement.remove();
        }

        // Create waiting message element
        waitingMessageElement = document.createElement('div');
        waitingMessageElement.classList.add('message', 'bot-message', 'waiting-message');
        
        const contentDiv = document.createElement('div');
        contentDiv.classList.add('message-content');
        contentDiv.innerHTML = '<p>Working on it...</p>';
        
        const senderLabel = document.createElement('div');
        senderLabel.classList.add('sender-label');
        senderLabel.textContent = 'Assistant';
        
        waitingMessageElement.appendChild(senderLabel);
        waitingMessageElement.appendChild(contentDiv);
        
        // Append to chat container
        chatContainer.appendChild(waitingMessageElement);
        
        // Auto-scroll to bottom
        chatContainer.scrollTop = chatContainer.scrollHeight;
    }

    // Function to remove waiting message
    function removeWaitingMessage() {
        if (waitingMessageElement) {
            waitingMessageElement.remove();
            waitingMessageElement = null;
        }
    }

    // Load chat history when popup opens
    loadChatHistory();

    // Handle file selection
    uploadButton.addEventListener('click', () => {
        fileInput.click();
    });

    fileInput.addEventListener('change', (event) => {
        selectedFile = event.target.files[0];
        if (selectedFile) {
            fileNameDisplay.textContent = `Selected file: ${selectedFile.name}`;
        }
    });

    function formatResponse(text) {
        // Check if the text contains numbered points or bullet points
        if (text.includes('1.') || text.includes('•')) {
            const paragraphs = text.split('\n');
            let formattedHtml = '';
            let inList = false;
            
            paragraphs.forEach(paragraph => {
                paragraph = paragraph.trim();
                
                if (paragraph === '') {
                    if (inList) {
                        formattedHtml += '</ul>\n';
                        inList = false;
                    }
                    formattedHtml += '<br>';
                    return;
                }

                // Check for numbered points or bullet points
                if (paragraph.match(/^\d+\./)) {
                    if (!inList) {
                        formattedHtml += '<ul class="numbered-list">\n';
                        inList = true;
                    }
                    formattedHtml += `<li>${paragraph.replace(/^\d+\.\s*/, '')}</li>\n`;
                } else if (paragraph.startsWith('•')) {
                    if (!inList) {
                        formattedHtml += '<ul class="bullet-list">\n';
                        inList = true;
                    }
                    formattedHtml += `<li>${paragraph.substring(1).trim()}</li>\n`;
                } else {
                    if (inList) {
                        formattedHtml += '</ul>\n';
                        inList = false;
                    }
                    formattedHtml += `<p>${paragraph}</p>\n`;
                }
            });

            if (inList) {
                formattedHtml += '</ul>';
            }

            return formattedHtml;
        }

        // If no special formatting needed, wrap in paragraph tags
        return `<p>${text}</p>`;
    }

    async function sendMessage() {
        try {
            const message = messageInput.value.trim();
            if (!message && !selectedFile) return;

            // Add user message to chat
            await addMessageToChat('user', message);
            messageInput.value = '';

            // Show waiting message
            showWaitingMessage();

            let response;

            if (selectedFile) {
                // If there's a file, use FormData
                const formData = new FormData();
                formData.append('message', message);
                formData.append('file', selectedFile);

                response = await fetch('http://localhost:8000/agent-workflow/form', {
                    method: 'POST',
                    body: formData
                });

                // Clear file selection
                fileNameDisplay.textContent = '';
                selectedFile = null;
            } else {
                // Regular text message - use JSON format
                response = await fetch('http://localhost:8000/agent-workflow', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        query: message
                    })
                });
            }

            // Remove waiting message
            removeWaitingMessage();

            if (!response.ok) {
                const errorData = await response.text();
                console.error('Server Error:', errorData);
                throw new Error(`Server error: ${response.status}`);
            }

            const data = await response.json();
            
            // Handle different response formats
            const botResponse = data.response || data.error || 'No response from server';
            await addMessageToChat('bot', botResponse);

            // Update conversation ID if provided
            if (data.conversation_id) {
                conversationId = data.conversation_id;
            }

        } catch (error) {
            console.error('Error:', error);
            
            // Remove waiting message if there's an error
            removeWaitingMessage();
            
            await addMessageToChat('bot', 'Sorry, there was an error processing your request. Please try again.');
        }
    }

    async function addMessageToChat(sender, message) {
        const messageDiv = document.createElement('div');
        messageDiv.classList.add('message', `${sender}-message`);
        
        // Add timestamp
        const timestamp = new Date().toLocaleTimeString();
        const timeSpan = document.createElement('span');
        timeSpan.classList.add('timestamp');
        timeSpan.textContent = timestamp;
        
        // Create message content div
        const contentDiv = document.createElement('div');
        contentDiv.classList.add('message-content');
        
        // Format message if it's from bot
        if (sender === 'bot') {
            contentDiv.innerHTML = formatResponse(message);
        } else {
            contentDiv.textContent = message;
        }
        
        // Add sender label
        const senderLabel = document.createElement('div');
        senderLabel.classList.add('sender-label');
        senderLabel.textContent = sender === 'user' ? 'You' : 'Assistant';
        
        // Assemble message components
        messageDiv.appendChild(senderLabel);
        messageDiv.appendChild(contentDiv);
        messageDiv.appendChild(timeSpan);
        
        chatContainer.appendChild(messageDiv);
        
        // Auto-scroll to bottom
        chatContainer.scrollTop = chatContainer.scrollHeight;

        // Save message to history
        await saveChatMessage(sender, message, timestamp);
    }

    async function saveChatMessage(sender, message, timestamp) {
        try {
            const history = await getChatHistory();
            history.push({ sender, message, timestamp });
            await chrome.storage.local.set({ 'chatHistory': history });
        } catch (error) {
            console.error('Error saving chat history:', error);
        }
    }

    async function getChatHistory() {
        try {
            const result = await chrome.storage.local.get('chatHistory');
            return result.chatHistory || [];
        } catch (error) {
            console.error('Error getting chat history:', error);
            return [];
        }
    }

    async function loadChatHistory() {
        const history = await getChatHistory();
        chatContainer.innerHTML = ''; // Clear existing messages
        
        for (const msg of history) {
            const messageDiv = document.createElement('div');
            messageDiv.classList.add('message', `${msg.sender}-message`);
            
            const timeSpan = document.createElement('span');
            timeSpan.classList.add('timestamp');
            timeSpan.textContent = msg.timestamp;
            
            const contentDiv = document.createElement('div');
            contentDiv.classList.add('message-content');
            
            // Format message if it's from bot
            if (msg.sender === 'bot') {
                contentDiv.innerHTML = formatResponse(msg.message);
            } else {
                contentDiv.textContent = msg.message;
            }
            
            const senderLabel = document.createElement('div');
            senderLabel.classList.add('sender-label');
            senderLabel.textContent = msg.sender === 'user' ? 'You' : 'Assistant';
            
            messageDiv.appendChild(senderLabel);
            messageDiv.appendChild(contentDiv);
            messageDiv.appendChild(timeSpan);
            
            chatContainer.appendChild(messageDiv);
        }
        
        chatContainer.scrollTop = chatContainer.scrollHeight;
    }

    async function clearChatHistory() {
        try {
            // Clear local storage
            await chrome.storage.local.remove('chatHistory');
            chatContainer.innerHTML = '';
            conversationId = null;
    
            // Reset supervisor state on backend
            const response = await fetch('http://localhost:8000/reset-supervisor', {
                method: 'POST'
            });
    
            if (!response.ok) {
                throw new Error('Failed to reset conversation state');
            }
    
                
        } catch (error) {
            console.error('Error clearing chat history:', error);
            addMessageToChat('bot', 'Error clearing chat history. Please try again.');
        }
    }

    // Event listeners
    sendButton.addEventListener('click', sendMessage);
    clearButton.addEventListener('click', clearChatHistory);
    
    messageInput.addEventListener('keypress', (e) => {
        if (e.key === 'Enter' && !e.shiftKey) {
            e.preventDefault();
            sendMessage();
        }
    });

    // Keep input focus
    messageInput.addEventListener('blur', () => {
        setTimeout(() => {
            if (document.activeElement !== fileInput) {
                messageInput.focus();
            }
        }, 100);
    });

    // Initial focus
    messageInput.focus();
});