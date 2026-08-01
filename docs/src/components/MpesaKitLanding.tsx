import React, { useEffect } from 'react';
import styles from './MpesaKit.module.css';
import { BackgroundAnimation } from './BackgroundAnimation';
import { HeroSection } from './HeroSection';
import { StatsSection } from './StatsSection';
import { FeaturesSection } from './FeaturesSection';
import { SecuritySection } from './SecuritySection';
import { APIStatusSection } from './APIStatusSection';
import { DocumentationSection } from './DocumentationSection';
import { CTASection } from './CTASection';
import { Footer } from './Footer';



export const MpesaKitLanding: React.FC = () => {
  useEffect(() => {
    // Intersection Observer for animations
    const observerOptions = {
      threshold: 0.1,
      rootMargin: '0px 0px -50px 0px'
    };

    const observer = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          entry.target.classList.add(styles.visible);
        }
      });
    }, observerOptions);

    // Observe all fade-in-up elements
    document.querySelectorAll(`.${styles.fadeInUp}`).forEach(el => {
      observer.observe(el);
    });

    // Create floating particles effect
    const createParticle = () => {
      const particle = document.createElement('div');
      particle.style.position = 'fixed';
      particle.style.width = '4px';
      particle.style.height = '4px';
      particle.style.background = 'var(--mpesa-green)';
      particle.style.borderRadius = '50%';
      particle.style.pointerEvents = 'none';
      particle.style.opacity = '0.6';
      particle.style.zIndex = '-1';

      const startX = Math.random() * window.innerWidth;
      const startY = window.innerHeight + 10;

      particle.style.left = startX + 'px';
      particle.style.top = startY + 'px';

      document.body.appendChild(particle);

      // Animate particle
      let y = startY;
      const speed = 0.5 + Math.random() * 1;

      const animateParticle = () => {
        y -= speed;
        particle.style.top = y + 'px';

        if (y < -10) {
          particle.remove();
        } else {
          requestAnimationFrame(animateParticle);
        }
      };

      animateParticle();
    };

    // Create particles occasionally
    const particleInterval = setInterval(createParticle, 3000);

    return () => {
      observer.disconnect();
      clearInterval(particleInterval);
    };
  }, []);

  // Data for components
  const heroData = {
    title: "MpesaKit",
    description: "Open-source Python M-Pesa SDK built by the community, for the community. A reliable third-party library that simplifies M-Pesa integration with comprehensive validation, full sync/async support, and developer-friendly design.",
    badges: ["Open Source", "99%+ Test Coverage", "500+ Test Cases", "Sync & Async", "10,000+ Downloads"],
    primaryCTA: {
      text: "🚀 Get Started",
      href: "/intro"
    },
    secondaryCTA: {
      text: "⭐ Star on GitHub",
      href: "https://github.com/rafaeljohn9/mpesakit"
    }
  };

  const statsData = [
    { number: "99%+", label: "Test Coverage" },
    { number: "500+", label: "Test Cases" },
    { number: "100%", label: "Type Hints" },
    { number: "10,000+", label: "PyPI Downloads" },
    { number: "24/7", label: "API Monitoring" }
  ];

  const featuresData = {
    title: "Why Choose MpesaKit?",
    subtitle: "Community-built features designed for real-world M-Pesa integration challenges",
    features: [
      {
        icon: "🔍",
        title: "Advanced Data Validation",
        description: "Comprehensive request and response schema validation ensures data integrity before hitting M-Pesa APIs.",
        features: [
          "Pydantic-powered schema validation",
          "Phone number format validation",
          "Amount range verification",
          "Callback URL security checks"
        ]
      },
      {
        icon: "🛡️",
        title: "Production Security",
        description: "Enterprise-grade security features to protect your transactions and sensitive data.",
        features: [
          "IP whitelist for callback endpoints",
          "SSL certificate validation",
          "Request signing & verification",
          "Token refresh automation"
        ]
      },
      {
        icon: "🎯",
        title: "Developer Experience",
        description: "Designed for developers who value clean code, type safety, and comprehensive documentation.",
        features: [
          "100% type hints coverage",
          "IntelliSense-friendly APIs",
          "Detailed error messages",
          "Rich debugging information"
        ]
      },
      {
        icon: "⚡",
        title: "Full Async Support",
        description: "Every service ships with an async counterpart built on an async HTTP client, so you're never blocking your event loop.",
        features: [
          "AsyncMpesaClient mirrors the sync client 1:1",
          "Drop-in async services (AsyncStkPush, AsyncB2C, etc.)",
          "Built for FastAPI, Starlette, and asyncio apps",
          "asyncio.gather-friendly for batch requests"
        ]
      },
      {
        icon: "🔔",
        title: "Built-in Callback Processing",
        description: "Stop hand-parsing webhook payloads — validate and parse callbacks with one call, on either client.",
        features: [
          "client.process_stk_callback() for STK Push results",
          "Dedicated helpers for B2C, Reversal, Tax and more",
          "Typed Pydantic objects, not raw dicts",
          "Identical on MpesaClient and AsyncMpesaClient"
        ]
      },
      {
        icon: "📊",
        title: "Complete API Coverage",
        description: "Full support for all M-Pesa Daraja APIs, in both sync and async, with real-time status monitoring.",
        features: [
          "STK Push (Lipa Na M-Pesa)",
          "C2B, B2C, B2B transactions",
          "Transaction status queries",
          "Account balance checks"
        ]
      },
      {
        icon: "🧪",
        title: "Battle-Tested Quality",
        description: "Extensive testing ensures reliability in production environments.",
        features: [
          "500+ comprehensive test cases",
          "99%+ code coverage",
          "Integration test suite",
          "Performance benchmarks"
        ]
      }
    ]
  };

  const securityData = {
    title: "Security & Reliability",
    description: "MpesaKit implements industry best practices to ensure your M-Pesa integrations are secure and reliable in production environments.",
    securityFeatures: [
      {
        icon: "🛡️",
        title: "IP Whitelisting",
        description: "Restrict callback URLs to trusted IP addresses"
      },
      {
        icon: "🔐",
        title: "SSL Validation",
        description: "Enforce HTTPS and validate certificates"
      },
      {
        icon: "🔑",
        title: "Token Management",
        description: "Secure token storage and automatic refresh, for both sync and async clients"
      },
      {
        icon: "📝",
        title: "Request Signing",
        description: "Cryptographic signing of requests for authenticity"
      }
    ]
  };

  const statusData = {
    title: "API Status Monitor",
    subtitle: "Real-time status of M-Pesa Daraja APIs and MpesaKit services",
    statusItems: [
      {
        name: "Authorization",
        status: "working" as "working" | "down" | "maintenance",
        note: "OAuth token issuance and refresh endpoints operational (sync & async)",
        link: { text: "Authorization Docs", href: "/auth" }
      },
      {
        name: "Dynamic QR",
        status: "working" as "working" | "down" | "maintenance",
        note: "QR generation and validation working as expected",
        link: { text: "Dynamic QR Docs", href: "/dynamic-qr" }
      },
      {
        name: "M-Pesa Express",
        status: "working" as "working" | "down" | "maintenance",
        note: "STK Push / Lipa Na M-Pesa flow operational, including callback processing",
        link: { text: "M-Pesa Express Docs", href: "/mpesa-express/stk-push" }
      },
      {
        name: "Customer To Business (C2B)",
        status: "working" as "working" | "down" | "maintenance",
        note: "C2B payment endpoints accepting requests",
        link: { text: "C2B Docs", href: "/c2b" }
      },
      {
        name: "Business To Customer (B2C)",
        status: "working" as "working" | "down" | "maintenance",
        note: "B2C payouts processing normally",
        link: { text: "B2C Docs", href: "/b2c" }
      },
      {
        name: "Transaction Status",
        status: "working" as "working" | "down" | "maintenance",
        note: "Status queries returning timely responses",
        link: { text: "Transaction Status Docs", href: "/transaction-status" }
      },
      {
        name: "Account Balance",
        status: "working" as "working" | "down" | "maintenance",
        note: "Account balance checks operational",
        link: { text: "Account Balance Docs", href: "/account-balance" }
      },
      {
        name: "Reversals",
        status: "working" as "working" | "down" | "maintenance",
        note: "Reversal endpoints functional",
        link: { text: "Reversals Docs", href: "/reversal" }
      },
      {
        name: "Tax Remittance",
        status: "working" as "working" | "down" | "maintenance",
        note: "Tax remittance integration working",
        link: { text: "Tax Remittance Docs", href: "/tax-remittance" }
      },
      {
        name: "Business Pay Bill",
        status: "working" as "working" | "down" | "maintenance",
        note: "PayBill payment flows operational",
        link: { text: "Pay Bill Docs", href: "/business-paybill" }
      },
      {
        name: "Business Buy Goods",
        status: "working" as "working" | "down" | "maintenance",
        note: "BuyGoods payment flows operational",
        link: { text: "Buy Goods Docs", href: "/business-buygoods" }
      },
      {
        name: "Bill Manager",
        status: "maintenance" as "working" | "down" | "maintenance",
        note: "On hold per Safaricom API support",
        link: { text: "Bill Manager (notes)", href: "https://developer.safaricom.co.ke/APIs/BillManager" }
      },
      {
        name: "B2B Express Checkout",
        status: "maintenance" as "working" | "down" | "maintenance",
        note: "On hold per Safaricom API support",
        link: { text: "B2B Express (notes)", href: "https://developer.safaricom.co.ke/APIs/B2BExpressCheckout" }
      },
      {
        name: "B2C Account Top Up",
        status: "working" as "working" | "down" | "maintenance",
        note: "Account top-up flows functioning normally",
        link: { text: "B2C Top Up Docs", href: "/b2c-account-top-up" }
      },
      {
        name: "M-Pesa Ratiba",
        status: "maintenance" as "working" | "down" | "maintenance",
        note: "On hold per Safaricom API support",
        link: { text: "Ratiba (notes)", href: "https://developer.safaricom.co.ke/APIs/MpesaRatiba" }
      }
    ]
  };

  const docsData = {
    title: "Comprehensive Documentation",
    subtitle: "Everything you need to get started and scale your M-Pesa integration",
    docItems: [
      {
        icon: "📚",
        title: "Quick Start Guide",
        description: "Get up and running in minutes with our step-by-step guide and code examples.",
        link: {
          text: "Start Building",
          href: "/intro"
        }
      },
      {
        icon: "🔧",
        title: "API Reference",
        description: "Complete API documentation with parameters, responses, and error codes.",
        link: {
          text: "View APIs",
          href: "/auth"
        }
      },
    ]
  };

  const ctaData = {
    title: "Ready to Build?",
    description: "Join thousands of developers who trust MpesaKit for their M-Pesa integrations. Start building secure, reliable payment solutions today — sync or async.",
    primaryCTA: {
      text: "🚀 Get Started Now",
      href: "/installation"
    },
    secondaryCTA: {
      text: "📖 View Documentation",
      href: "/intro"
    }
  };

  const footerData = {
    title: "MpesaKit",
    description: "Open-source Python M-Pesa SDK built by the community, for the community.",
    sections: [
      {
        title: "Resources",
        links: [
          { text: "Quick Start", href: "/intro" },
          { text: "API Reference", href: "/auth" },
          { text: "Contributing", href: "https://github.com/RafaelJohn9/mpesakit/blob/master/CONTRIBUTING.md" }
        ]
      },
      {
        title: "Community",
        links: [
          { text: "GitHub", href: "https://github.com/rafaeljohn9/mpesakit" },
          { text: "Issues", href: "https://github.com/rafaeljohn9/mpesakit/issues" },
          { text: "Discussions", href: "https://github.com/rafaeljohn9/mpesakit/discussions" },
          { text: "Discord", href: "https://discord.gg/vzGBh5Qu" },
          { text: "PyPI", href: "https://pypi.org/project/mpesakit/" },
          { text: "Download Stats", href: "https://pepy.tech/project/mpesakit" }
        ]
      },
      {
        title: "Support",
        links: [
          { text: "Report Bug", href: "https://github.com/rafaeljohn9/mpesakit/issues" },
          { text: "Get Help", href: "https://github.com/rafaeljohn9/mpesakit/discussions" },
        ]
      }
    ],
    socialLinks: [
      { text: "📱", href: "https://github.com/rafaeljohn9/mpesakit" },
      { text: "📦", href: "https://pypi.org/project/mpesakit/" },
      { text: "📚", href: "/intro" }
    ],
    copyright: `© ${new Date().getFullYear()} MpesaKit. Open source project by ByteBarn.`,
    tagline: "Built with ❤️ for the community. 10,000+ downloads and counting."
  };

  return (
    <div className={styles.mpesaKitLanding}>
      <BackgroundAnimation />
      <HeroSection {...heroData} />
      <StatsSection stats={statsData} />
      <FeaturesSection {...featuresData} />
      <SecuritySection {...securityData} />
      <APIStatusSection {...statusData} />
      <DocumentationSection {...docsData} />
      <CTASection {...ctaData} />
      <Footer {...footerData} />
    </div>
  );
};