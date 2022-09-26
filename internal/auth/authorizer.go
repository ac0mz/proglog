package auth

import (
	"fmt"

	"github.com/casbin/casbin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// New はモデル(ACLファイル)とポリシー(CSVファイル)のパスをCasbinに設定したAuthorizerを返却する。
func New(model, policy string) *Authorizer {
	enforcer := casbin.NewEnforcer(model, policy)
	return &Authorizer{
		enforcer: enforcer,
	}
}

type Authorizer struct {
	enforcer *casbin.Enforcer
}

// Authorize はACLによる認可を実施し、拒否された場合はエラーを返却する。
// Casbinで設定したモデルとポリシーに基づき、サブジェクトがオブジェクトに対するアクションの実行を許可されているかを検証する。
func (a *Authorizer) Authorize(subject, object, action string) error {
	if !a.enforcer.Enforce(subject, object, action) {
		msg := fmt.Sprintf(
			"%s not permitted to %s to %s",
			subject, object, action,
		)
		st := status.New(codes.PermissionDenied, msg)
		return st.Err()
	}
	return nil
}
