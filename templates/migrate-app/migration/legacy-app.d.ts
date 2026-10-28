// The old accessors the composition root adds through legacyApp and requestApp.
import "%%next%%";
declare module "%%next%%" {
  interface Register {
    legacyApp: { getCrud(): any };
  }
}
