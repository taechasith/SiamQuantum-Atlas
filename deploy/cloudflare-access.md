# Cloudflare Access for Admin Panel

This adds an identity-aware layer in front of the admin UI and admin APIs.
Application-level RBAC still remains the source of truth.

## Recommended setup

1. Put the Docker stack behind a Cloudflare Tunnel.
2. In Cloudflare Zero Trust → Access → Applications, create two Self-hosted applications:
   - `atlas.example.com/admin*`
   - `atlas.example.com/api/admin/*`
3. Add an Allow policy:
   - Emails: admin reviewers only, or
   - Email domain: the university domain, or
   - IdP group: `siamquantum-admins`
4. Require MFA for the policy.
5. Set session duration to a short window (for example 8 hours).
6. Test in a private window with a non-allowlisted account. Cloudflare should block before the app loads.

## Keep app-level RBAC

Cloudflare Access does not replace `role` checks inside the application:

- `users.role = 'admin'` in the database controls admin APIs.
- `/api/admin/*` returns `403` for non-admin sessions.
- Vue route guard redirects non-admin users away from `/admin`.
- Admin nav item is hidden for non-admin users.
- Admin actions are written to `admin_audit_log`.

## Local testing

localhost does not pass through Cloudflare Access. Test the application layer directly:

1. Login as the admin account.
2. Use Admin → Access Control to promote or demote an existing account.
3. Login as a promoted account and confirm `/admin` is reachable.
4. Login as a normal account and confirm `/admin` redirects to `/dashboard` and admin APIs return `403`.
