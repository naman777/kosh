// The dashboard talks to the lab supervisor (cmd/lab) through /lab/*, which
// Next proxies, so the browser only ever sees one origin.
const labURL = process.env.LAB_URL || 'http://localhost:8095';

/** @type {import('next').NextConfig} */
const nextConfig = {
  output: 'standalone',
  async rewrites() {
    return [{ source: '/lab/:path*', destination: `${labURL}/lab/:path*` }];
  },
};

export default nextConfig;
