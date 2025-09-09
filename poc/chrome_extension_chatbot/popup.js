document.addEventListener('DOMContentLoaded', function() {
  const chatContainer = document.getElementById('chat-container');
  const userInput = document.getElementById('user-input');
  const sendButton = document.getElementById('send-button');
  const apiKeyInput = document.getElementById('api-key-input');
  const saveKeyButton = document.getElementById('save-key-button');
  const typingIndicator = document.querySelector('.typing-indicator');

  let apiKey = '';

  // Load saved API key
  chrome.storage.local.get(['openai_api_key'], function(result) {
    if (result.openai_api_key) {
      apiKey = result.openai_api_key;
      apiKeyInput.value = '********';
    }
  });

  // Save API key
  saveKeyButton.addEventListener('click', function() {
    apiKey = apiKeyInput.value;
    chrome.storage.local.set({ 'openai_api_key': apiKey }, function() {
      apiKeyInput.value = '********';
      alert('API key saved!');
    });
  });

  function addMessage(message, isUser) {
    const messageDiv = document.createElement('div');
    messageDiv.className = `message ${isUser ? 'user-message' : 'bot-message'}`;
    messageDiv.textContent = message;
    chatContainer.appendChild(messageDiv);
    chatContainer.scrollTop = chatContainer.scrollHeight;
  }

  async function getChatGPTResponse(userMessage) {
    if (!apiKey) {
      return 'Please enter your OpenAI API key first.';
    }

    try {
      const response = await fetch('https://api.openai.com/v1/chat/completions', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${apiKey}`
        },
        body: JSON.stringify({
          model: "gpt-3.5-turbo",
          messages: [
            {
              role: "user",
              content: userMessage
            }
          ],
          max_tokens: 150
        })
      });

      if (!response.ok) {
        throw new Error('API request failed');
      }

      const data = await response.json();
      return data.choices[0].message.content.trim();
    } catch (error) {
      console.error('Error:', error);
      return 'Sorry, there was an error getting a response. Please check your API key and try again.';
    }
  }

  async function handleUserInput() {
    const message = userInput.value.trim();
    if (message) {
      // Disable input and button while processing
      userInput.disabled = true;
      sendButton.disabled = true;
      
      // Add user message
      addMessage(message, true);
      userInput.value = '';

      // Show typing indicator
      typingIndicator.style.display = 'block';

      // Get and display ChatGPT response
      const botResponse = await getChatGPTResponse(message);
      typingIndicator.style.display = 'none';
      addMessage(botResponse, false);

      // Re-enable input and button
      userInput.disabled = false;
      sendButton.disabled = false;
      userInput.focus();
    }
  }

  sendButton.addEventListener('click', handleUserInput);
  userInput.addEventListener('keypress', function(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleUserInput();
    }
  });
});
