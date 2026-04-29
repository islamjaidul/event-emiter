import type { Config } from 'jest';

const config: Config = {
  projects: [
    {
      displayName: 'unit',
      testEnvironment: 'node',
      transform: { '^.+\\.ts$': ['ts-jest', { tsconfig: 'tsconfig.json' }] },
      testMatch: ['<rootDir>/tests/unit/**/*.test.ts'],
      moduleFileExtensions: ['ts', 'js'],
    },
    {
      displayName: 'e2e',
      testEnvironment: 'node',
      transform: { '^.+\\.ts$': ['ts-jest', { tsconfig: 'tsconfig.json' }] },
      testMatch: ['<rootDir>/tests/e2e/**/*.e2e.test.ts'],
      testTimeout: 120000,
      moduleFileExtensions: ['ts', 'js'],
    },
  ],
};

export default config;
