import React, { useEffect } from "react";

import { useConfig } from "config";
import AnalyticsServiceProvider, { useAnalytics } from "hooks/useAnalytics";
import useSegment from "hooks/useSegment";
import { useGetService } from "core/servicesProvider";

function WithAnalytics({ customerId }: { customerId: string }) {
  const config = useConfig();

  // segment section
  useSegment(config.segment.enabled ? config.segment.token : "");
  const analyticsService = useAnalytics();
  useEffect(() => {
    analyticsService.identify(customerId);
  }, [analyticsService, customerId]);

  return null;
}

const AnalyticsInitializer: React.FC<{
  children: React.ReactNode;
}> = ({ children }) => {
  const customerIdProvider = useGetService<
    () => { userId: string; workspaceId: string }
  >("useCustomerIdProvider");

  const { userId, workspaceId } = customerIdProvider();
  const config = useConfig();

  return (
    <AnalyticsServiceProvider
      context={{ userId, workspaceId }}
      version={config.version}
    >
      <WithAnalytics customerId={userId} />
      {children}
    </AnalyticsServiceProvider>
  );
};

export { AnalyticsInitializer };
