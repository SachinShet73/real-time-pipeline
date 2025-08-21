class PeopleDashboard {
    constructor() {
        this.socket = null;
        this.people = [];
        this.isConnected = false;
        this.init();
    }

    init() {
        this.connectToSocket();
        this.loadInitialData();
        this.setupEventListeners();
    }

    connectToSocket() {
        // Use relative URL for Docker environment
        this.socket = io();

        this.socket.on('connect', () => {
            console.log('Connected to server');
            this.isConnected = true;
            this.updateConnectionStatus(true);
        });

        this.socket.on('disconnect', () => {
            console.log('Disconnected from server');
            this.isConnected = false;
            this.updateConnectionStatus(false);
        });

        this.socket.on('new_person', (person) => {
            console.log('New person received:', person);
            this.addNewPerson(person);
        });

        this.socket.on('connected', (data) => {
            console.log('Server message:', data);
        });
    }

    async loadInitialData() {
        try {
            // Use relative URL for Docker environment
            const response = await fetch('/api/people');
            if (response.ok) {
                this.people = await response.json();
                this.renderPeople();
                this.updateStats();
                document.getElementById('loading').style.display = 'none';
            } else {
                throw new Error('Failed to fetch data');
            }
        } catch (error) {
            console.error('Error loading data:', error);
            document.getElementById('loading').innerHTML = '❌ Failed to load data. Please check if the backend is running.';
        }
    }

    addNewPerson(person) {
        // Check if person already exists
        const exists = this.people.some(p => p.id === person.id);
        if (!exists) {
            this.people.unshift(person); // Add to beginning
            this.renderNewPerson(person);
            this.updateStats();
            
            // Show notification
            this.showNotification(`New person added: ${person.first_name} ${person.last_name}`);
        }
    }

    renderPeople() {
        const grid = document.getElementById('people-grid');
        grid.innerHTML = '';
        
        this.people.forEach((person, index) => {
            const personCard = this.createPersonCard(person);
            personCard.style.animationDelay = `${index * 0.1}s`;
            grid.appendChild(personCard);
        });
    }

    renderNewPerson(person) {
        const grid = document.getElementById('people-grid');
        const personCard = this.createPersonCard(person);
        personCard.classList.add('new');
        
        // Insert at the beginning
        grid.insertBefore(personCard, grid.firstChild);
        
        // Remove new class after animation
        setTimeout(() => {
            personCard.classList.remove('new');
        }, 1000);
    }

    createPersonCard(person) {
        const card = document.createElement('div');
        card.className = 'person-card';
        card.innerHTML = `
            <div class="person-header">
                <img src="${person.picture || 'https://via.placeholder.com/60'}" 
                     alt="${person.first_name}" class="person-avatar">
                <div>
                    <div class="person-name">${person.first_name} ${person.last_name}</div>
                    <div class="person-username">@${person.username}</div>
                </div>
            </div>
            <div class="person-details">
                <div class="detail-row">
                    <span class="detail-icon">📧</span>
                    <span class="detail-text">${person.email}</span>
                </div>
                <div class="detail-row">
                    <span class="detail-icon">📱</span>
                    <span class="detail-text">${person.phone}</span>
                </div>
                <div class="detail-row">
                    <span class="detail-icon">🏠</span>
                    <span class="detail-text">${person.address}</span>
                </div>
                <div class="detail-row">
                    <span class="detail-icon">📮</span>
                    <span class="detail-text">${person.post_code}</span>
                </div>
                <div class="detail-row">
                    <span class="detail-icon">⚤</span>
                    <span class="detail-text">${person.gender}</span>
                </div>
                <div class="detail-row">
                    <span class="detail-icon">📅</span>
                    <span class="detail-text">${this.formatDate(person.registered_date)}</span>
                </div>
            </div>
        `;
        return card;
    }

    updateStats() {
        document.getElementById('total-count').textContent = this.people.length;
        
        // Calculate new today (simplified - just showing recent additions)
        const today = new Date().toISOString().split('T')[0];
        const newToday = this.people.filter(person => 
            person.registered_date && person.registered_date.startsWith(today)
        ).length;
        document.getElementById('new-today').textContent = newToday;

        // Update online status
        const statusIndicator = document.getElementById('online-status');
        statusIndicator.textContent = this.isConnected ? '🟢' : '🔴';
        statusIndicator.style.color = this.isConnected ? '#4CAF50' : '#f44336';
    }

    updateConnectionStatus(connected) {
        const status = document.getElementById('status');
        if (connected) {
            status.textContent = 'Connected - Live Updates Active';
            status.className = 'status-indicator status-connected';
        } else {
            status.textContent = 'Disconnected - No Live Updates';
            status.className = 'status-indicator status-disconnected';
        }
        this.updateStats();
    }

    formatDate(dateString) {
        if (!dateString) return 'Unknown';
        try {
            return new Date(dateString).toLocaleDateString('en-US', {
                year: 'numeric',
                month: 'short',
                day: 'numeric'
            });
        } catch {
            return dateString;
        }
    }

    showNotification(message) {
        // Create a simple notification
        const notification = document.createElement('div');
        notification.style.cssText = `
            position: fixed;
            top: 80px;
            right: 20px;
            background: #4CAF50;
            color: white;
            padding: 15px 20px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            z-index: 1000;
            animation: slideIn 0.3s ease;
        `;
        notification.textContent = message;
        document.body.appendChild(notification);

        // Remove after 3 seconds
        setTimeout(() => {
            notification.style.animation = 'slideOut 0.3s ease';
            setTimeout(() => {
                document.body.removeChild(notification);
            }, 300);
        }, 3000);

        // Add CSS animations if not already present
        if (!document.getElementById('notification-styles')) {
            const style = document.createElement('style');
            style.id = 'notification-styles';
            style.textContent = `
                @keyframes slideIn {
                    from { transform: translateX(100%); opacity: 0; }
                    to { transform: translateX(0); opacity: 1; }
                }
                @keyframes slideOut {
                    from { transform: translateX(0); opacity: 1; }
                    to { transform: translateX(100%); opacity: 0; }
                }
            `;
            document.head.appendChild(style);
        }
    }

    setupEventListeners() {
        // Add any additional event listeners here
        window.addEventListener('beforeunload', () => {
            if (this.socket) {
                this.socket.disconnect();
            }
        });
    }
}

// Initialize the dashboard when the page loads
document.addEventListener('DOMContentLoaded', () => {
    new PeopleDashboard();
});