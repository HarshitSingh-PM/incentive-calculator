module.exports = {
  apps: [{
    name: 'incentive-calculator',
    script: 'server.js',
    env: {
      NODE_ENV: 'production',
      PORT: 3456,
    },
    instances: 1,
    autorestart: true,
    watch: false,
    max_memory_restart: '256M',
  }],
};
