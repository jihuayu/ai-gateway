const path = require('node:path');

module.exports = {
  plugins: {
    '@stylexjs/postcss-plugin': {
      cwd: __dirname,
      include: ['src/**/*.{ts,tsx}'],
      babelConfig: {
        babelrc: false,
        parserOpts: {
          plugins: ['typescript', 'jsx'],
        },
        plugins: [
          [
            '@stylexjs/babel-plugin',
            {
              dev: false,
              test: false,
              runtimeInjection: false,
              genConditionalClasses: true,
              treeshakeCompensation: true,
              unstable_moduleResolution: {
                type: 'commonJS',
                rootDir: path.resolve(__dirname),
              },
            },
          ],
        ],
      },
      useCSSLayers: false,
    },
  },
};
